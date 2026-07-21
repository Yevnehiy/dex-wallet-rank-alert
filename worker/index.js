// ============================================================
// Rank Alert — Cloudflare Worker (Unbound)
// 2026-07-13: CoinGecko paid plan hit its MONTHLY CREDIT LIMIT (overage off) —
// every pro-api call now 429s. Flipped USE_PAID_API back to false to run on the
// free tier (api.geckoterminal.com + api.coingecko.com, no key) at zero cost
// until the credit resets. Flip back to true (and POLL_MS=1min, GECKO_DELAY_MS=500,
// cron="* * * * *") once it does — see crypto.md for the full incident writeup.
// ============================================================

const USE_PAID_API = false;

const GECKO_BASE        = USE_PAID_API ? 'https://pro-api.coingecko.com/api/v3/onchain' : 'https://api.geckoterminal.com/api/v2';
const COINGECKO_BASE    = USE_PAID_API ? 'https://pro-api.coingecko.com/api/v3'         : 'https://api.coingecko.com/api/v3';
const RANK_THRESHOLD    = 4;
const RANK_FROM_FLOOR   = 10;
const WINDOW_MS         = 60 * 60 * 1000;
const POLL_MS           = USE_PAID_API ? 1 * 60 * 1000 : 20 * 60 * 1000;
const TOP_N             = 20;
const GECKO_DELAY_MS    = USE_PAID_API ? 500 : 12000; // free tier: 10 req/min shared bucket (7500ms was too close to the limit per earlier testing)
export const MIN_TRADE_USD      = 200;
export const MIN_WALLET_VOL_USD = 5000;
export const PENDING_TTL_S      = 3600;       // 60 min window to confirm

// Confirmation (checked every minute while pending) — either branch confirms:
//   - wallet's own detected volume >= 0.1% of the token's CURRENT market cap, OR
//   - price has moved >=3% in the signal's direction since detection
// PLUS a direction gate (user rule 2026-07-18): the price path must agree with
// the signal side for the whole pending lifetime — a sell can NEVER confirm
// while the price is above the detection reference (and vice versa), no matter
// what the mcap branch says; a counter-move beyond DIR_INVALIDATE_PCT kills
// the pending outright instead of letting it wait out the move.
const MIN_VOL_MCAP_RATIO  = 0.001;
const PRICE_CONFIRM_PCT   = 3.0;
const DIR_INVALIDATE_PCT  = 2.0;

// Stats epoch v3 (2026-07-19T17:00Z): fresh restart AFTER the full rule set went
// live — direction gate, m5≥5%, headline 3/15, anti-wash filter. Only signals
// ENTERED from this moment count in /stats and /analytics; the ~748 prior v2
// records still exist in KV but are gated out by ts, so nothing pre-anti-wash
// pollutes the clean backtest. Gate is by entry ts, so in-flight opens don't count.
const STATS_START_TS = Date.UTC(2026, 6, 19, 17, 0, 0); // 2026-07-19T17:00:00Z

// v2 signal tracking: instead of closing at a fixed ±5%, each signal records a
// price trail until WIDE bounds or timeout, then a whole SL/TP grid is replayed
// from the trail — this feeds /analytics ("which SL%/TP% is actually best").
// The headline 5/5 combo still drives statsSummary/win-loss for continuity.
const WIDE_PCT           = 15;              // close tracking at ±15%
const MAX_TRACK_H        = 48;              // ...or after 48h
const TRAIL_MIN_GAP_MS   = 5 * 60 * 1000;   // record a point at most each 5 min
const TRAIL_MIN_MOVE_PCT = 1;               // ...and only if price moved ≥1%
const TRAIL_MAX_POINTS   = 400;
const SL_GRID = [2, 3, 4, 5, 7, 10];
const TP_GRID = [2, 3, 4, 5, 7, 10, 15];

// Backtest log — every confirmed signal is logged and tracked to SL/TP resolution.
// Headline 3%/15% (user decision 2026-07-18, from the /analytics grid over 413
// closed dynamic-board signals: 3/15 → +0.25R vs symmetric 5/5 → −0.19R; the
// momentum universe rewards tight stop + wide take). The old static-list era
// preferred 5/5 — different token selection, different regime.
const SIGLOG_SL_PCT  = 3;              // stop loss = -1R
const SIGLOG_TP_PCT  = 15;             // take profit
const SIGLOG_R_WIN   = SIGLOG_TP_PCT / SIGLOG_SL_PCT; // win = +5R at 3/15
const SIGLOG_TTL_S   = 90 * 24 * 3600; // keep resolved logs 90 days

// Named exports below (fetchTrades, aggregate*, detect*, key helpers, constants)
// are consumed by scripts/scanner.mjs — the local PC scanner that took over
// detection after GeckoTerminal blocked Cloudflare's egress IPs (2026-07-15).
export const STABLECOIN_SYMBOLS = new Set([
  'USDT','USDC','DAI','BUSD','TUSD','FRAX','FDUSD','PYUSD','EURC','EUTBL',
  'GHO','USDG','USDY','OUSG','RLUSD','USDM','STABLE','USTB','USAT','USDA',
  'USDAI','USDTB','USD0','AUSD','REUSD','CRVUSD','SATUSD','EURCV','USDF',
  'PAXG','XAUT','KAU','KAG','APXUSD','USDD','EURSAFO','JAAA','JTRSY',
  'EARNETH','M','U','BORG',
  'USDS','USDE','SUSDE','SDAI','USDX','GUSDC','LISUSD','EURE','EUROE',
]);

const CHAIN_PRIORITY = [
  ['ethereum',            'eth'],
  ['arbitrum-one',        'arbitrum'],
  ['base',                'base'],
  ['binance-smart-chain', 'bsc'],
  ['polygon-pos',         'polygon_pos'],
  ['optimistic-ethereum', 'optimism'],
  ['avalanche',           'avax'],
];

const KV_TOKEN_LIST = 'tokens';
const snapKey     = (addr, bucket) => `snap:${addr}:${bucket}`; // combined buyers+sellers+price — 1 write/token/poll instead of 3
export const pendingKey  = (side, alertType, tokenAddr, wallet) => `pending:${side}:${alertType}:${tokenAddr}:${wallet}`;
export const cooldownKey = (side, alertType, tokenAddr, wallet) => `cooldown:${side}:${alertType}:${tokenAddr}:${wallet}`;
// status is embedded in the key prefix (siglog:open:... vs siglog:win:/loss:...) so
// checkOpenSignals/the /stats endpoint can list only open positions via kv.list,
// instead of reading every historical (already-resolved) entry every minute forever.
const siglogKey   = (status, tokenAddr, ts, wallet) => `siglog:${status}:${tokenAddr}:${ts}:${wallet}`;
const STATS_KEY   = 'statsSummary';
const timeBucket  = () => Math.floor(Date.now() / POLL_MS) * POLL_MS;

// After a signal confirms for a given wallet+token+side, don't re-alert on the
// same ongoing activity for a while — at 1-min polling, a wallet mid-way through
// a sustained sell/buy can otherwise re-qualify and re-confirm every 1-3 minutes,
// spamming many "signals" for what is really one continuous market event.
const SIGNAL_COOLDOWN_S = WINDOW_MS / 1000; // 60 min

const sleep = ms => new Promise(r => setTimeout(r, ms));

// EVM addresses are case-insensitive → normalize to lowercase for use as keys.
// Solana base58 addresses are case-SENSITIVE and never start with "0x" — keep as-is.
export const normAddr = a => (a && a.startsWith('0x')) ? a.toLowerCase() : a;

// ============================================================
// CoinGecko price (cached 5 min by CF edge)
// ============================================================

// CoinGecko started rejecting requests without a User-Agent with 403
// (rolled out ~2026-07-15 16:00 UTC — froze all confirmations/resolutions
// until this header was added). Workers' fetch() sends no UA by default.
const UA_HEADER = { 'User-Agent': 'rank-alert/1.0' };

// debug scaffolding (2026-07-16): last upstream HTTP statuses seen by
// fetchTokenPrice, surfaced into debug:pendingCron/debug:openCron.
export const priceDebug = { cg: null, gt: null };

async function fetchTokenPrice(coinId, fallbackAddr, fallbackNetwork, apiKey) {
  const authHeaders = USE_PAID_API ? { ...UA_HEADER, 'x-cg-pro-api-key': apiKey } : UA_HEADER;
  if (coinId) {
    const res = await fetch(
      `${COINGECKO_BASE}/simple/price?ids=${coinId}&vs_currencies=usd&include_24hr_change=true`,
      { headers: authHeaders, cf: { cacheTtl: 300 } }
    );
    priceDebug.cg = res.status;
    if (res.ok) {
      const data = await res.json();
      if (data[coinId]) return data[coinId];
    }
  }
  // Fallback: onchain token price
  if (fallbackAddr && fallbackNetwork) {
    const res = await fetch(
      `${GECKO_BASE}/networks/${fallbackNetwork}/tokens/${encodeURIComponent(fallbackAddr)}`,
      { headers: { Accept: 'application/json;version=20230302', ...authHeaders }, cf: { cacheTtl: 300 } }
    );
    priceDebug.gt = res.status;
    if (!res.ok) return null;
    const data = await res.json();
    const attrs = data.data?.attributes;
    if (!attrs?.price_usd) return null;
    // Paid Onchain API's /tokens/{addr} doesn't include price_change_percentage —
    // don't default a missing value to 0% (that would show a false "24h: +0.0%").
    const rawChange = attrs.price_change_percentage?.h24;
    const change = rawChange != null ? parseFloat(rawChange) : NaN;
    return { usd: parseFloat(attrs.price_usd), usd_24h_change: isNaN(change) ? null : change };
  }
  return null;
}

function fmtPrice(p) {
  if (!p || p.usd == null) return null;
  const price = p.usd;
  const change = p.usd_24h_change;
  const priceStr = price < 0.0001 ? `$${price.toFixed(8)}`
                 : price < 1      ? `$${price.toFixed(4)}`
                 :                  `$${price.toFixed(2)}`;
  const changeStr = change != null ? `  |  24h: ${change >= 0 ? '+' : ''}${change.toFixed(1)}%` : '';
  return `${priceStr}${changeStr}`;
}

// ============================================================
// Pool-based trades endpoint — free (GeckoTerminal) or paid (CoinGecko
// Onchain, x-cg-pro-api-key) depending on USE_PAID_API.
// Returns up to 300 latest trades for a pool
// ============================================================

export async function fetchTrades(poolAddr, network, apiKey) {
  const url = `${GECKO_BASE}/networks/${network}/pools/${encodeURIComponent(poolAddr)}/trades` +
              `?trade_volume_in_usd_greater_than=${MIN_TRADE_USD}`;
  const headers = { Accept: 'application/json;version=20230302', 'User-Agent': 'rank-alert/1.0' };
  if (USE_PAID_API) headers['x-cg-pro-api-key'] = apiKey;

  // 2026-07-13: free-tier 429s are now coming in bursts even at the historically
  // safe 12s pacing — root cause is contention on Cloudflare's shared egress IP
  // pool (other tenants), not our own request rate. One retry after a short pause
  // recovers most of these since the external contention is transient.
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, { headers, cf: { cacheTtl: 0 } });
    if (res.status === 429) {
      if (attempt === 0) { await sleep(5000); continue; }
      throw new Error('GeckoTerminal rate limit — increase GECKO_DELAY_MS');
    }
    if (res.status === 404) return [];
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`CoinGecko Onchain ${res.status}: ${text.slice(0, 200)}`);
    }
    const data = await res.json();
    return data.data ?? [];
  }
}

// Reference price from most recent trade (free — no extra API call). Must match
// from_token_address/to_token_address against our tokenAddr — price_from_in_usd
// is only our token's price when our token is the "from" side of that trade
// (a 'buy' trade has from=quote token, to=our token, and vice versa for 'sell').
export function extractRefPrice(rawTrades, tokenAddr) {
  const a = rawTrades[0]?.attributes;
  if (!a) return null;
  const addr = normAddr(tokenAddr);
  let p;
  if (normAddr(a.from_token_address) === addr) p = parseFloat(a.price_from_in_usd ?? 0);
  else if (normAddr(a.to_token_address) === addr) p = parseFloat(a.price_to_in_usd ?? 0);
  else p = NaN;
  return (isNaN(p) || p <= 0) ? null : { usd: p, usd_24h_change: null };
}

// ============================================================
// Aggregate raw trades → [{address, rank, volume_usd, txs}]
// txs kept in-memory for single-buy filter (no extra API call)
// ============================================================

export function aggregateTopBuyers(trades) {
  const since = Date.now() - WINDOW_MS;
  const walletMap = new Map();

  for (const t of trades) {
    const a = t.attributes;
    if (a.kind !== 'buy') continue;
    if (new Date(a.block_timestamp).getTime() < since) continue;

    const addr = normAddr(a.tx_from_address);
    if (!addr || addr === '0x0000000000000000000000000000000000000000') continue;

    const vol = parseFloat(a.volume_in_usd) || 0;
    if (!walletMap.has(addr)) walletMap.set(addr, { address: addr, volume_usd: 0, txs: [], paymentTokenAddr: null, lastTxHash: null });
    const w = walletMap.get(addr);
    w.volume_usd += vol;
    w.txs.push(vol);
    // trades are newest-first, so the first match per wallet is their most recent
    // buy — that's the currency (stablecoin/wrapped asset) they're currently paying with,
    // and its tx_hash, kept for manual on-chain verification (not shown in the alert).
    if (w.paymentTokenAddr == null && a.from_token_address) w.paymentTokenAddr = normAddr(a.from_token_address);
    if (w.lastTxHash == null && a.tx_hash) w.lastTxHash = a.tx_hash;
  }

  return [...walletMap.values()]
    .sort((a, b) => b.volume_usd - a.volume_usd)
    .slice(0, TOP_N)
    .map((w, i) => ({ address: w.address, rank: i + 1, volume_usd: w.volume_usd, txs: w.txs, paymentTokenAddr: w.paymentTokenAddr, lastTxHash: w.lastTxHash }));
}

export function aggregateTopSellers(trades) {
  const since = Date.now() - WINDOW_MS;
  const walletMap = new Map();

  for (const t of trades) {
    const a = t.attributes;
    if (a.kind !== 'sell') continue;
    if (new Date(a.block_timestamp).getTime() < since) continue;

    const addr = normAddr(a.tx_from_address);
    if (!addr || addr === '0x0000000000000000000000000000000000000000') continue;

    const vol = parseFloat(a.volume_in_usd) || 0;
    if (!walletMap.has(addr)) walletMap.set(addr, { address: addr, volume_usd: 0, txs: [], lastTxHash: null });
    const w = walletMap.get(addr);
    w.volume_usd += vol;
    w.txs.push(vol);
    // trades are newest-first — first match per wallet is their most recent sell tx.
    if (w.lastTxHash == null && a.tx_hash) w.lastTxHash = a.tx_hash;
  }

  return [...walletMap.values()]
    .sort((a, b) => b.volume_usd - a.volume_usd)
    .slice(0, TOP_N)
    .map((w, i) => ({ address: w.address, rank: i + 1, volume_usd: w.volume_usd, txs: w.txs, lastTxHash: w.lastTxHash }));
}

// ============================================================
// KV snapshot management
// ============================================================

function slimTraders(traders) {
  return traders.map(({ address, rank, volume_usd }) => ({ address, rank, volume_usd }));
}

async function saveCombinedSnapshot(kv, tokenAddr, buyers, sellers) {
  await kv.put(
    snapKey(tokenAddr, timeBucket()),
    JSON.stringify({ buyers: slimTraders(buyers), sellers: slimTraders(sellers) }),
    { expirationTtl: 7200 }
  );
}

async function loadHistoricalCombined(kv, tokenAddr) {
  const raw = await kv.get(snapKey(tokenAddr, timeBucket() - WINDOW_MS));
  return raw ? JSON.parse(raw) : null;
}

// ============================================================
// Market cap — used only at confirmation time (checkPendingAlerts).
// Tokens without a CoinGecko ID have no market cap and can never confirm.
// ============================================================

async function fetchMarketCap(coinId, apiKey) {
  if (!coinId) return null;
  try {
    const headers = USE_PAID_API ? { ...UA_HEADER, 'x-cg-pro-api-key': apiKey } : UA_HEADER;
    const res = await fetch(
      `${COINGECKO_BASE}/simple/price?ids=${coinId}&vs_currencies=usd&include_market_cap=true`,
      { headers, cf: { cacheTtl: 300 } }
    );
    if (!res.ok) return null;
    const data = await res.json();
    return data[coinId]?.usd_market_cap ?? null;
  } catch {
    return null;
  }
}

// ============================================================
// Pending alert system — store candidate, confirm once wallet volume
// is >= 0.1% of the token's current market cap, OR price moves 3%+ in the
// signal's direction (checked every minute).
// ============================================================

async function storePendingAlert(kv, side, alertType, tokenAddr, walletAddr, jump, tokenSymbol, network, tokenId, referencePrice) {
  const key = pendingKey(side, alertType, tokenAddr, walletAddr);
  const existing = await kv.get(key);
  if (existing) return; // already waiting

  const onCooldown = await kv.get(cooldownKey(side, alertType, tokenAddr, walletAddr));
  if (onCooldown) return; // this wallet already triggered+confirmed recently — same ongoing activity, don't spam

  await kv.put(key, JSON.stringify({
    side, alertType, tokenAddr, tokenId, tokenSymbol, network,
    jump,
    referencePrice: referencePrice?.usd ?? null,
  }), { expirationTtl: PENDING_TTL_S });
  console.log(`Pending: ${tokenSymbol} ${side} ${alertType} vol=$${jump.volume_usd.toFixed(0)} ref=$${referencePrice?.usd}`);
}

// Prices/mcaps relayed by the local scanner (scripts/scanner.mjs) — since
// ~2026-07-15 CoinGecko and GeckoTerminal 429 everything from Cloudflare's
// egress at cron volumes, so direct fetches only serve as a fallback.
const RELAY_MAX_AGE_MS = 15 * 60 * 1000;

async function loadRelay(kv) {
  const out = { prices: {}, mcaps: {}, quotes: {} };
  try { const raw = await kv.get('prices:dex'); if (raw) out.prices = JSON.parse(raw); } catch {}
  try { const raw = await kv.get('mcaps:cg'); if (raw) out.mcaps = JSON.parse(raw); } catch {}
  try { const raw = await kv.get('quotes:usd'); if (raw) out.quotes = JSON.parse(raw); } catch {}
  return out;
}

// USD value of an alert balance: the alerted token prices via its own current
// price; payment currencies via stables (1:1) or scanner-relayed quote prices.
const QUOTE_ALIAS = { WETH: 'ETH', ETH: 'ETH', WBNB: 'BNB', BNB: 'BNB', SOL: 'SOL', WSOL: 'SOL', WAVAX: 'AVAX', WMATIC: 'POL', WBTC: 'BTC', CBBTC: 'BTC' };

function balanceUsd(balanceInfo, tokenPriceUsd, quotes) {
  if (tokenPriceUsd != null) return balanceInfo.balance * tokenPriceUsd;
  const sym = balanceInfo.symbol;
  if (STABLECOIN_SYMBOLS.has(sym)) return balanceInfo.balance;
  const q = quotes?.[QUOTE_ALIAS[sym] ?? sym];
  return q > 0 ? balanceInfo.balance * q : null;
}

async function checkPendingAlerts(kv, botToken, chatId, apiKey) {
  // debug counters (2026-07-16): confirmations froze silently — persist per-run
  // visibility into KV since wrangler tail shows empty logs for this path.
  const dbg = { at: Date.now(), seen: 0, priceOk: 0, mcapOk: 0, confirmed: 0, err: null };
  try {
  const relay = await loadRelay(kv);
  const priceCache = new Map();
  const marketCapCache = new Map();
  let result = await kv.list({ prefix: 'pending:', limit: 100 });
  while (true) {
    for (const { name: keyName } of result.keys) {
      const raw = await kv.get(keyName);
      if (!raw) continue;
      const p = JSON.parse(raw);
      dbg.seen++;

      const cacheKey = p.tokenId || p.tokenAddr;
      if (!priceCache.has(cacheKey)) {
        const rp = relay.prices[p.tokenAddr];
        const price = (rp && Date.now() - rp.ts < RELAY_MAX_AGE_MS)
          ? { usd: rp.usd, usd_24h_change: null }
          : await fetchTokenPrice(p.tokenId, p.tokenAddr, p.network, apiKey).catch(() => null);
        priceCache.set(cacheKey, price);
      }
      const priceInfo = priceCache.get(cacheKey);
      if (priceInfo?.usd) dbg.priceOk++;

      // Direction gate — evaluated BEFORE any confirmation branch.
      const pctNow = (p.referencePrice != null && priceInfo?.usd)
        ? (priceInfo.usd - p.referencePrice) / p.referencePrice * 100
        : null;
      if (pctNow != null) {
        const counterPct = p.side === 'sell' ? pctNow : -pctNow; // >0 = ціна йде проти сигналу
        if (counterPct > DIR_INVALIDATE_PCT) {
          await kv.delete(keyName);
          dbg.invalidated = (dbg.invalidated ?? 0) + 1;
          console.log(`Invalidated ${p.tokenSymbol} ${p.side} — price ${pctNow.toFixed(2)}% against the signal`);
          continue;
        }
        if (counterPct > 0) continue; // проти сигналу, але в межах толерансу — чекаємо, не підтверджуємо
      }

      // Branch A: wallet's own detected volume >= 0.1% of CURRENT market cap.
      if (!marketCapCache.has(p.tokenId)) {
        const rm = p.tokenId ? relay.mcaps[p.tokenId] : null;
        const mcap = (rm && Date.now() - rm.ts < RELAY_MAX_AGE_MS)
          ? rm.mcap
          : (p.tokenId ? await fetchMarketCap(p.tokenId, apiKey).catch(() => null) : null);
        marketCapCache.set(p.tokenId, mcap);
      }
      const marketCap = marketCapCache.get(p.tokenId);
      if (marketCap) dbg.mcapOk++;
      const volConfirmed = marketCap && marketCap > 0 && p.jump.volume_usd >= marketCap * MIN_VOL_MCAP_RATIO;

      // Branch B: price has moved >=3% in the signal's direction since detection.
      const priceConfirmed = pctNow != null && (
        (p.side === 'buy' && pctNow >= PRICE_CONFIRM_PCT) ||
        (p.side === 'sell' && pctNow <= -PRICE_CONFIRM_PCT)
      );

      if (!volConfirmed && !priceConfirmed) continue;
      if (!priceInfo?.usd) continue; // still need a price to display/log the alert

      await sendAlert(botToken, chatId, p.jump, p.tokenSymbol, p.network, p.tokenAddr, p.side, p.alertType, priceInfo, p.via, relay.quotes);
      await logSignal(kv, p, priceInfo.usd);
      await kv.put(cooldownKey(p.side, p.alertType, p.tokenAddr, p.jump.address), '1', { expirationTtl: SIGNAL_COOLDOWN_S });
      await kv.delete(keyName);
      dbg.confirmed++;
      const reason = volConfirmed ? `vol=$${p.jump.volume_usd.toFixed(0)} mcap=$${marketCap.toFixed(0)}` : `price ${pctNow.toFixed(2)}%`;
      console.log(`Confirmed ${p.tokenSymbol} ${p.side} (${reason})`);
    }
    if (result.list_complete) break;
    result = await kv.list({ prefix: 'pending:', limit: 100, cursor: result.cursor });
  }
  } catch (e) { dbg.err = String(e.message ?? e).slice(0, 200); }
  dbg.upstream = { ...priceDebug };
  await kv.put('debug:pendingCron', JSON.stringify(dbg));
}

// ============================================================
// Backtest log — record every confirmed signal, track to SL/TP
// SL/TP % come from SIGLOG_SL_PCT / SIGLOG_TP_PCT above (buy=long, sell=short)
// ============================================================

async function logSignal(kv, p, entryPrice) {
  if (entryPrice == null) return;
  const ts = Date.now();
  await kv.put(siglogKey('open', p.tokenAddr, ts, p.jump.address), JSON.stringify({
    v: 2,
    symbol: p.tokenSymbol, side: p.side, alertType: p.alertType,
    tokenAddr: p.tokenAddr, network: p.network, tokenId: p.tokenId,
    via: p.via ?? null, viaClass: p.viaClass ?? null,
    wallet: p.jump.address, entryPrice, ts, status: 'open',
    txHash: p.jump.lastTxHash ?? null,
    trail: [[ts, entryPrice]],
  }), { expirationTtl: SIGLOG_TTL_S });
}

// Replay the price trail against every SL/TP combo. First threshold crossed
// decides; if both cross within one sample, SL wins (pessimistic, same as live).
function replayGrid(trail, entryPrice, side) {
  const dir = side === 'buy' ? 1 : -1;
  const pnl = trail.map(([, price]) => (price - entryPrice) / entryPrice * 100 * dir);
  const grid = {};
  for (const sl of SL_GRID) {
    for (const tp of TP_GRID) {
      let out = 'open';
      for (const x of pnl) {
        if (x <= -sl) { out = 'loss'; break; }
        if (x >= tp)  { out = 'win'; break; }
      }
      grid[`${sl}/${tp}`] = out;
    }
  }
  return grid;
}

async function updateStats(kv, outcome) {
  const raw = await kv.get(STATS_KEY);
  const stats = raw ? JSON.parse(raw) : { wins: 0, losses: 0, totalR: 0 };
  if (outcome === 'win') { stats.wins++; stats.totalR += SIGLOG_R_WIN; }
  else                   { stats.losses++; stats.totalR -= 1; }
  stats.lastUpdated = Date.now();
  await kv.put(STATS_KEY, JSON.stringify(stats));
}

async function checkOpenSignals(kv, apiKey) {
  const dbg = { at: Date.now(), seen: 0, priceOk: 0, resolved: 0, err: null };
  try {
  const relay = await loadRelay(kv);
  const priceCache = new Map();
  let result = await kv.list({ prefix: 'siglog:open:', limit: 100 });
  while (true) {
    for (const { name: keyName } of result.keys) {
      const raw = await kv.get(keyName);
      if (!raw) continue;
      const s = JSON.parse(raw);
      dbg.seen++;

      const cacheKey = s.tokenId || s.tokenAddr;
      if (!priceCache.has(cacheKey)) {
        const rp = relay.prices[s.tokenAddr];
        const price = (rp && Date.now() - rp.ts < RELAY_MAX_AGE_MS)
          ? { usd: rp.usd, usd_24h_change: null }
          : await fetchTokenPrice(s.tokenId, s.tokenAddr, s.network, apiKey).catch(() => null);
        priceCache.set(cacheKey, price);
      }
      const priceInfo = priceCache.get(cacheKey);
      if (!priceInfo?.usd) continue;
      dbg.priceOk++;
      const price = priceInfo.usd;
      const now = Date.now();

      // Legacy (pre-v2) records: resolve at the old fixed ±5%, never counted in stats.
      if (s.v !== 2) {
        const isBuy = s.side === 'buy';
        const hitSL = isBuy ? price <= s.entryPrice * (1 - SIGLOG_SL_PCT / 100) : price >= s.entryPrice * (1 + SIGLOG_SL_PCT / 100);
        const hitTP = isBuy ? price >= s.entryPrice * (1 + SIGLOG_TP_PCT / 100) : price <= s.entryPrice * (1 - SIGLOG_TP_PCT / 100);
        if (!hitSL && !hitTP) continue;
        const outcome = hitSL ? 'loss' : 'win';
        s.status = outcome; s.resolvedAt = now; s.resolvedPrice = price;
        await kv.put(siglogKey(outcome, s.tokenAddr, s.ts, s.wallet), JSON.stringify(s), { expirationTtl: SIGLOG_TTL_S });
        await kv.delete(keyName);
        dbg.resolved++;
        continue;
      }

      // v2: keep tracking until ±WIDE_PCT or timeout, recording a sparse trail.
      const dirSign = s.side === 'buy' ? 1 : -1;
      const pnlPct = (price - s.entryPrice) / s.entryPrice * 100 * dirSign;
      let dirty = false;

      const last = s.trail[s.trail.length - 1];
      const movedPct = Math.abs(price - last[1]) / last[1] * 100;
      if (now - last[0] >= TRAIL_MIN_GAP_MS && movedPct >= TRAIL_MIN_MOVE_PCT && s.trail.length < TRAIL_MAX_POINTS) {
        s.trail.push([now, price]);
        dirty = true;
      }

      // Headline (5/5) outcome is decided LIVE the moment ±5% is crossed — the
      // per-minute check is finer than the sparse trail, and stats shouldn't lag
      // 48h behind. SL wins when both crossed between checks (pessimistic).
      if (!s.headlineDone) {
        if (pnlPct <= -SIGLOG_SL_PCT)     s.headlineDone = 'loss';
        else if (pnlPct >= SIGLOG_TP_PCT) s.headlineDone = 'win';
        if (s.headlineDone) {
          dirty = true;
          if (s.ts >= STATS_START_TS) await updateStats(kv, s.headlineDone);
          console.log(`Headline ${s.symbol} ${s.side} ${s.headlineDone} at ${pnlPct.toFixed(1)}% (tracking continues)`);
        }
      }

      const done = pnlPct >= WIDE_PCT || pnlPct <= -WIDE_PCT || now - s.ts >= MAX_TRACK_H * 3600 * 1000;
      if (!done) {
        if (dirty) await kv.put(keyName, JSON.stringify(s), { expirationTtl: SIGLOG_TTL_S });
        continue;
      }

      if (s.trail[s.trail.length - 1][1] !== price) s.trail.push([now, price]); // closing price always in trail
      s.grid = replayGrid(s.trail, s.entryPrice, s.side);
      const status = s.headlineDone ?? 'flat'; // flat = ±5% ніколи не зачепило за весь трек
      s.status = status; s.resolvedAt = now; s.resolvedPrice = price;
      await kv.put(siglogKey(status, s.tokenAddr, s.ts, s.wallet), JSON.stringify(s), { expirationTtl: SIGLOG_TTL_S });
      await kv.delete(keyName);
      dbg.resolved++;
      console.log(`Siglog v2 closed: ${s.symbol} ${s.side} ${status} pnl=${pnlPct.toFixed(1)}% trail=${s.trail.length}`);
    }
    if (result.list_complete) break;
    result = await kv.list({ prefix: 'siglog:open:', limit: 100, cursor: result.cursor });
  }
  } catch (e) { dbg.err = String(e.message ?? e).slice(0, 200); }
  await kv.put('debug:openCron', JSON.stringify(dbg));
}

// ============================================================
// Scanner heartbeat watchdog — detection runs on the user's PC
// (scripts/scanner.mjs) because GeckoTerminal blocks Cloudflare egress IPs.
// If the scanner's heartbeat goes stale, wallet detection is down even though
// this worker keeps confirming/resolving — tell the user (max 1 warning/hour).
// ============================================================

const HEARTBEAT_STALE_MS = 15 * 60 * 1000;

async function checkScannerHeartbeat(kv, botToken, chatId) {
  const raw = await kv.get('heartbeat:scanner');
  if (!raw) return; // scanner never ran yet — stay silent pre-launch
  const age = Date.now() - parseInt(raw);
  if (isNaN(age) || age < HEARTBEAT_STALE_MS) return;
  if (await kv.get('warned:scanner')) return;
  await kv.put('warned:scanner', '1', { expirationTtl: 3600 });
  const mins = Math.round(age / 60000);
  await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text: `⚠️ Сканер мовчить ${mins} хв — детекція не працює (ПК вимкнений або сканер упав?). Підтвердження відкритих сигналів працюють далі.` }),
  });
}

// ============================================================
// Rank-jump detection
// ============================================================

export function detectJumps(current, historical) {
  if (!historical) return [];
  const histMap = new Map(historical.map(t => [t.address, t.rank]));
  return current
    .filter(t => {
      if (t.rank > RANK_THRESHOLD) return false;
      if (t.volume_usd < MIN_WALLET_VOL_USD) return false;
      const prev = histMap.get(t.address);
      return prev === undefined || prev > RANK_FROM_FLOOR;
    })
    .map(t => ({ ...t, prevRank: histMap.get(t.address) ?? null }));
}

export function detectStepUp(current, historical) {
  if (!historical) return [];
  const histMap = new Map(historical.map(t => [t.address, t.rank]));
  return current
    .filter(t => {
      if (t.rank < 5 || t.rank > 10) return false;
      if (t.volume_usd < MIN_WALLET_VOL_USD) return false;
      const prev = histMap.get(t.address);
      return prev !== undefined && prev - t.rank >= 2;
    })
    .map(t => ({ ...t, prevRank: histMap.get(t.address) }));
}

export function detectActivity(current) {
  return current.filter(t => {
    if (t.rank < 1 || t.rank > 5) return false;
    if (t.volume_usd < MIN_WALLET_VOL_USD) return false;
    const condA = t.txs.length >= 3 && t.volume_usd > 10000;  // repeated trader
    const condB = t.txs.length > 10;                          // very active bot/trader
    const condC = t.volume_usd > 50000;                       // single large position ($50k+)
    return condA || condB || condC;
  });
}

// ============================================================
// On-chain wallet balance — free public RPC, eth_call to balanceOf()/decimals().
// Used to show what a wallet has left after a sell (remaining token balance)
// or what they're paying with when buying (remaining stablecoin/wrapped-asset
// balance of whichever currency they most recently used for this token).
// ============================================================

// PublicNode — verified working + correct chainId for all 7 networks (2026-07-12).
const RPC_URLS = {
  eth:         'https://ethereum.publicnode.com',
  base:        'https://base.publicnode.com',
  bsc:         'https://bsc.publicnode.com',
  arbitrum:    'https://arbitrum-one.publicnode.com',
  polygon_pos: 'https://polygon-bor.publicnode.com',
  optimism:    'https://optimism.publicnode.com',
  avax:        'https://avalanche-c-chain.publicnode.com',
  // publicnode's solana gateway serves HTML for getTokenAccountsByOwner (verified
  // 2026-07-15) — the official endpoint supports both methods we need.
  solana:      'https://api.mainnet-beta.solana.com',
};

// Well-known stablecoin/wrapped-asset addresses per chain, just for a nicer label
// in the alert — falls back to a shortened address if the payment token isn't here.
const KNOWN_QUOTE_TOKENS = {
  eth: {
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
    '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT',
    '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
    '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH',
  },
  base: {
    '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913': 'USDC',
    '0x4200000000000000000000000000000000000006': 'WETH',
  },
  bsc: {
    '0x55d398326f99059ff775485246999027b3197955': 'USDT',
    '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d': 'USDC',
    '0xe9e7cea3dedca5984780bafc599bd69add087d56': 'BUSD',
    '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c': 'WBNB',
  },
  arbitrum: {
    '0xaf88d065e77c8cc2239327c5edb3a432268e5831': 'USDC',
    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9': 'USDT',
    '0x82af49447d8a07e3bd95bd0d56f35241523fbab1': 'WETH',
  },
  polygon_pos: {
    '0x3c499c542cef5e3811e1192ce70d8cc03d5c3359': 'USDC',
    '0xc2132d05d31c914a87c6611c10748aeb04b58e8f': 'USDT',
    '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270': 'WMATIC',
  },
  optimism: {
    '0x0b2c639c533813f4aa9d7837caf62653d097ff85': 'USDC',
    '0x4200000000000000000000000000000000000006': 'WETH',
  },
  avax: {
    '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e': 'USDC',
    '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7': 'WAVAX',
  },
  solana: {
    'So11111111111111111111111111111111111111112': 'SOL',
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'USDC',
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 'USDT',
  },
};

async function ethCall(rpcUrl, to, data) {
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_call', params: [{ to, data }, 'latest'] }),
  });
  if (!res.ok) return null;
  const json = await res.json();
  return json.result ?? null;
}

async function solRpc(rpcUrl, method, params) {
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params }),
  });
  if (!res.ok) return null;
  return res.json();
}

const WSOL_MINT = 'So11111111111111111111111111111111111111112';

async function getSolanaTokenBalance(rpcUrl, mint, walletAddr) {
  try {
    // wSOL trades are funded from native SOL — the wallet's real "ammo" is its
    // native balance, not a (usually auto-closed) wSOL token account.
    if (mint === WSOL_MINT) {
      const json = await solRpc(rpcUrl, 'getBalance', [walletAddr]);
      const lamports = json?.result?.value;
      if (lamports == null) return null;
      return { balance: lamports / 1e9, symbol: 'SOL' };
    }
    const json = await solRpc(rpcUrl, 'getTokenAccountsByOwner', [walletAddr, { mint }, { encoding: 'jsonParsed' }]);
    const accounts = json?.result?.value;
    if (!accounts) return null;
    const balance = accounts.reduce((s, a) => s + (a.account?.data?.parsed?.info?.tokenAmount?.uiAmount ?? 0), 0);
    const symbol = KNOWN_QUOTE_TOKENS.solana[mint] ?? `${mint.slice(0, 4)}...${mint.slice(-4)}`;
    return { balance, symbol };
  } catch {
    return null;
  }
}

async function getTokenBalance(network, tokenAddr, walletAddr) {
  const rpcUrl = RPC_URLS[network];
  if (!rpcUrl || !tokenAddr) return null;
  if (network === 'solana') return getSolanaTokenBalance(rpcUrl, tokenAddr, walletAddr);
  try {
    const balanceOfData = '0x70a08231' + walletAddr.toLowerCase().replace('0x', '').padStart(64, '0');
    const [balHex, decHex] = await Promise.all([
      ethCall(rpcUrl, tokenAddr, balanceOfData),
      ethCall(rpcUrl, tokenAddr, '0x313ce567'), // decimals()
    ]);
    if (!balHex || !decHex) return null;
    const decimals = parseInt(decHex, 16);
    const balance = Number(BigInt(balHex)) / (10 ** decimals);
    const symbol = KNOWN_QUOTE_TOKENS[network]?.[normAddr(tokenAddr)]
      ?? `${tokenAddr.slice(0, 6)}...${tokenAddr.slice(-4)}`;
    return { balance, symbol };
  } catch {
    return null;
  }
}

function fmtBalance(n) {
  if (n === 0)         return '0';
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000)     return `${(n / 1_000).toFixed(1)}k`;
  if (n >= 1)         return n.toFixed(2);
  return n.toFixed(6);
}

// ============================================================
// Telegram
// ============================================================

function fmtUsd(n) {
  if (!n) return '?';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000)     return `$${(n / 1_000).toFixed(0)}k`;
  return `$${n.toFixed(0)}`;
}

function explorerUrl(network, addr) {
  const map = {
    eth:         `https://etherscan.io/address/${addr}`,
    bsc:         `https://bscscan.com/address/${addr}`,
    arbitrum:    `https://arbiscan.io/address/${addr}`,
    polygon_pos: `https://polygonscan.com/address/${addr}`,
    optimism:    `https://optimistic.etherscan.io/address/${addr}`,
    base:        `https://basescan.org/address/${addr}`,
    avax:        `https://snowtrace.io/address/${addr}`,
    solana:      `https://solscan.io/account/${addr}`,
  };
  return map[network] ?? `https://etherscan.io/address/${addr}`;
}

const TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

// The tx sender is often just the operator EOA — the tokens usually live in a
// different wallet/contract (verified on-chain 2026-07-16: 13 of 15 sampled
// alerts had ZERO token flow through tx.from, hence the constant "0 balance").
// Parse the trade receipt: the largest-value Transfer of the token names the
// real holder (largest wins to skip BSC tax/fee split transfers).
async function findBeneficiary(network, tokenAddr, txHash, side) {
  const rpcUrl = RPC_URLS[network];
  if (!rpcUrl || network === 'solana') return null;
  const res = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_getTransactionReceipt', params: [txHash] }),
  });
  if (!res.ok) return null;
  const receipt = (await res.json()).result;
  const logs = (receipt?.logs ?? []).filter(l =>
    l.address?.toLowerCase() === tokenAddr.toLowerCase() &&
    l.topics?.[0] === TRANSFER_TOPIC && l.topics.length >= 3);
  if (!logs.length) return null;
  let best = null, bestVal = -1n;
  for (const l of logs) {
    let v; try { v = BigInt(l.data && l.data !== '0x' ? l.data : '0x0'); } catch { v = 0n; }
    if (v > bestVal) { bestVal = v; best = l; }
  }
  // buy: token leaves the pool → `to` of the biggest transfer is the holder;
  // sell: token enters the pool → `from` is the holder.
  const addr = '0x' + (side === 'buy' ? best.topics[2] : best.topics[1]).slice(26).toLowerCase();
  return addr === '0x0000000000000000000000000000000000000000' ? null : addr;
}

// Chain colour badges. Telegram messages have no CSS, so a colour = a coloured
// emoji shape (user request 2026-07-19): base = blue square, eth = blue diamond,
// bsc = yellow, solana = purple. Only these 4 chains are scanned (POOLMAP_NETWORKS).
const CHAIN_BADGE = {
  base: '🟦', eth: '🔷', bsc: '🟨', solana: '🟪',
};
// Alert TYPE emoji is now independent of side (side has its own 🟢/🔴) so the
// three detection types read at a glance regardless of buy/sell.
const ALERT_TYPE_META = {
  jump:     { emoji: '🚀', label: 'JUMP · ривок у топ' },
  stepup:   { emoji: '📈', label: 'STEP-UP · +2 позиції' },
  activity: { emoji: '🔔', label: 'ACTIVITY · топ-5 актив' },
};
// Timeframe that put the token under watch (from `via`): fast/mid/slow board.
function viaEmoji(via) {
  if (!via) return '';
  if (via.startsWith('m5'))  return '⚡';
  if (via.startsWith('m30')) return '🕒';
  if (via.startsWith('h24')) return '🗓';
  return '•';
}

async function sendAlert(botToken, chatId, jump, tokenSymbol, network, tokenAddr, side, alertType, priceInfo, via, quotes) {
  const shortAddr = `${jump.address.slice(0, 6)}...${jump.address.slice(-4)}`;
  const isBuy     = side === 'buy';
  const badge     = CHAIN_BADGE[network] ?? '⬜';
  const tm        = ALERT_TYPE_META[alertType] ?? { emoji: '❓', label: alertType };

  // Row 1 — chain badge + token + direction; Row 2 — alert type + how it entered pending.
  const titleRow = `${badge} <b>${tokenSymbol}</b> · ${network.toUpperCase()}   ${isBuy ? '🟢' : '🔴'} <b>${isBuy ? 'BUY' : 'SELL'}</b>`;
  const typeRow  = `${tm.emoji} <b>${tm.label}</b>` + (via ? `\n${viaEmoji(via)} через <i>${via}</i>` : '');

  let detail;
  if (alertType === 'jump') {
    const fromStr = jump.prevRank ? `#${jump.prevRank}` : 'поза топ-20';
    detail = `Ранг ${fromStr} → <b>#${jump.rank}</b> за 60хв · Обсяг ${fmtUsd(jump.volume_usd)}`;
  } else if (alertType === 'stepup') {
    detail = `Ранг #${jump.prevRank} → <b>#${jump.rank}</b> (+${jump.prevRank - jump.rank}) · Обсяг ${fmtUsd(jump.volume_usd)}`;
  } else {
    detail = `Ранг <b>#${jump.rank}</b> · ${jump.txs.length} tx · Обсяг ${fmtUsd(jump.volume_usd)}`;
  }

  const priceStr = fmtPrice(priceInfo);
  const parts = [
    titleRow,
    typeRow,
    ``,
    `Гаманець: <code>${shortAddr}</code>`,
    detail,
  ];
  if (priceStr) parts.push(`Ціна: ${priceStr}`);

  // Real holder of the tokens (may differ from the executing wallet) + its
  // remaining balance: for a sell — token left; for a buy — what they paid with.
  let holder = null;
  if (jump.lastTxHash) holder = await findBeneficiary(network, tokenAddr, jump.lastTxHash, side).catch(() => null);
  const balanceOwner = holder ?? jump.address;
  if (holder && holder !== jump.address) {
    parts.push(`Утримувач: <code>${holder.slice(0, 6)}...${holder.slice(-4)}</code> (≠ виконавець)`);
  }
  const balanceTokenAddr = isBuy ? jump.paymentTokenAddr : tokenAddr;
  const balanceInfo = await getTokenBalance(network, balanceTokenAddr, balanceOwner).catch(() => null);
  if (balanceInfo) {
    const label = isBuy ? 'Залишок валюти покупки' : `Залишок ${tokenSymbol}`;
    const zeroNote = (!isBuy && balanceInfo.balance === 0) ? ' — вийшов повністю' : '';
    // dollars first (user request 2026-07-16), raw units in parentheses
    const usd = balanceUsd(balanceInfo, isBuy ? null : priceInfo?.usd, quotes);
    const amountStr = `${fmtBalance(balanceInfo.balance)} ${balanceInfo.symbol}`;
    parts.push(usd != null
      ? `${label}: ${usd === 0 ? '$0' : fmtUsd(usd)} (${amountStr})${zeroNote}`
      : `${label}: ${amountStr}${zeroNote}`);
  }

  const links = [
    `<a href="https://www.geckoterminal.com/${network}/tokens/${tokenAddr}">GeckoTerminal</a>`,
    `<a href="${explorerUrl(network, jump.address)}">Explorer</a>`,
  ];
  if (holder && holder !== jump.address) links.push(`<a href="${explorerUrl(network, holder)}">Holder</a>`);
  parts.push(``, links.join(' | '));

  const text = parts.join('\n');

  const res = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', disable_web_page_preview: true }),
  });
  if (!res.ok) console.error(`Telegram ${res.status}: ${await res.text().catch(() => '')}`);
}

// ============================================================
// Per-token processing — uses pool address + CoinGecko API key
// ============================================================

async function processToken({ address: tokenAddr, network, symbol, pool, id }, kv, botToken, chatId, apiKey) {
  if (!pool) {
    console.warn(`${symbol}: no pool address — skipping`);
    return;
  }
  if (STABLECOIN_SYMBOLS.has(symbol)) return;

  const rawTrades = await fetchTrades(pool, network, apiKey);
  if (rawTrades.length === 0) return;

  const refPrice = extractRefPrice(rawTrades, tokenAddr);

  // One combined historical read + one combined write (buyers+sellers) instead
  // of separate KV keys — cuts write volume at 1-min polling.
  const histCombined = await loadHistoricalCombined(kv, tokenAddr);
  const histBuyers    = histCombined?.buyers ?? null;
  const histSellers   = histCombined?.sellers ?? null;

  const buyers  = aggregateTopBuyers(rawTrades);
  const sellers = aggregateTopSellers(rawTrades);
  await saveCombinedSnapshot(kv, tokenAddr, buyers, sellers);

  // No gate here — every detected wallet becomes a pending candidate.
  // Confirmation (volume >=0.1% market cap OR price moved 3%+) happens in
  // checkPendingAlerts, not at detection time.

  // --- Buyers ---
  for (const jump of detectJumps(buyers, histBuyers)) {
    await storePendingAlert(kv, 'buy', 'jump', tokenAddr, jump.address, jump, symbol, network, id, refPrice);
  }
  for (const entry of detectStepUp(buyers, histBuyers)) {
    await storePendingAlert(kv, 'buy', 'stepup', tokenAddr, entry.address, entry, symbol, network, id, refPrice);
  }
  for (const entry of detectActivity(buyers)) {
    await storePendingAlert(kv, 'buy', 'activity', tokenAddr, entry.address, entry, symbol, network, id, refPrice);
  }

  // --- Sellers ---
  for (const jump of detectJumps(sellers, histSellers)) {
    await storePendingAlert(kv, 'sell', 'jump', tokenAddr, jump.address, jump, symbol, network, id, refPrice);
  }
  for (const entry of detectStepUp(sellers, histSellers)) {
    await storePendingAlert(kv, 'sell', 'stepup', tokenAddr, entry.address, entry, symbol, network, id, refPrice);
  }
  for (const entry of detectActivity(sellers)) {
    await storePendingAlert(kv, 'sell', 'activity', tokenAddr, entry.address, entry, symbol, network, id, refPrice);
  }
}

// ============================================================
// Token list
// ============================================================

async function getTokenList(kv) {
  // Try new generic key first, fall back to old keys for compatibility
  const raw = await kv.get(KV_TOKEN_LIST)
           ?? await kv.get('top30_145:tokens')
           ?? await kv.get('top300:tokens');
  return raw ? JSON.parse(raw) : null;
}

// ============================================================
// Auto-update token list (runs weekly via cron "0 3 * * 7")
// Ranks tokens by top-pool 24h DEX volume, filtered to Binance Futures listings.
// FROM_RANK / TO_RANK from wrangler.toml [vars] are 0-based slice
// indices into the sorted unique-token list:
//   Account 1: FROM=0  TO=50  → top 50 tokens by DEX vol
//   Account 2: FROM=50 TO=100 → tokens 51-100
// ============================================================

export const SKIP_SYMBOLS = new Set([
  ...STABLECOIN_SYMBOLS,
  'WETH','ETH','WBTC','BTC','WBNB','BNB','WMATIC','MATIC','WAVAX','AVAX',
  'WSTETH','STETH','CBETH','RETH','WBETH','CBBTC','WEETH','EZETH','RSETH',
]);

const UPDATE_NETWORKS   = ['eth', 'base', 'bsc']; // arbitrum/polygon_pos/optimism/avax dropped 2026-07-15 — negligible DEX-volume share, cut for API budget
const PAGES_PER_NETWORK = 8;   // 20 pools/page × 8 = 160 per network
const MIN_POOL_VOL_USD  = 50000;
const UPDATE_DELAY_MS   = USE_PAID_API ? 500 : 12000;

async function getBinanceFuturesSymbols() {
  const res = await fetch('https://fapi.binance.com/fapi/v1/exchangeInfo', { cf: { cacheTtl: 3600 } });
  if (!res.ok) { console.error(`Binance exchangeInfo: ${res.status}`); return new Set(); }
  const data = await res.json();
  const symbols = new Set();
  for (const s of data.symbols) {
    if (s.contractType !== 'PERPETUAL' || s.quoteAsset !== 'USDT' || s.status !== 'TRADING') continue;
    const base = s.baseAsset.toUpperCase();
    symbols.add(base);
    // Handle 1000PEPE → PEPE, 1000000MOG → MOG
    const stripped = base.replace(/^\d+/, '');
    if (stripped && stripped !== base) symbols.add(stripped);
  }
  return symbols;
}

async function updateTokenList(env) {
  const { RANK_STORE, COINGECKO_API_KEY } = env;
  const FROM_RANK = parseInt(env.FROM_RANK ?? '0');
  const TO_RANK   = parseInt(env.TO_RANK   ?? '70');
  console.log(`updateTokenList: DEX vol indices ${FROM_RANK}–${TO_RANK - 1}`);

  const binanceFutures = await getBinanceFuturesSymbols();
  console.log(`Binance Futures filter: ${binanceFutures.size} symbols`);

  const allTokens = [];
  let reqCount = 0;

  for (const network of UPDATE_NETWORKS) {
    for (let page = 1; page <= PAGES_PER_NETWORK; page++) {
      if (reqCount > 0) await sleep(UPDATE_DELAY_MS);

      const poolsHeaders = { Accept: 'application/json;version=20230302' };
      if (USE_PAID_API) poolsHeaders['x-cg-pro-api-key'] = COINGECKO_API_KEY;
      const res = await fetch(
        `${GECKO_BASE}/networks/${network}/pools?sort=h24_volume_usd_desc&page=${page}&include=base_token,quote_token`,
        { headers: poolsHeaders, cf: { cacheTtl: 0 } }
      );
      reqCount++;
      if (!res.ok) { console.error(`pools ${network} p${page}: ${res.status}`); continue; }

      const data = await res.json();

      const tokenLookup = new Map();
      for (const item of (data.included ?? [])) {
        if (item.type !== 'token') continue;
        tokenLookup.set(item.id, {
          address: item.attributes.address?.toLowerCase(),
          symbol:  item.attributes.symbol?.toUpperCase() ?? '?',
          id:      item.attributes.coingecko_coin_id ?? null,
        });
      }

      for (const pool of (data.data ?? [])) {
        const poolAddr = pool.attributes?.address?.toLowerCase();
        const h24Vol   = parseFloat(pool.attributes?.volume_usd?.h24 ?? 0);
        if (!poolAddr || h24Vol < MIN_POOL_VOL_USD) continue;

        const baseTok  = tokenLookup.get(pool.relationships?.base_token?.data?.id);
        const quoteTok = tokenLookup.get(pool.relationships?.quote_token?.data?.id);

        let main = null;
        if (baseTok?.address && !SKIP_SYMBOLS.has(baseTok.symbol))  main = baseTok;
        else if (quoteTok?.address && !SKIP_SYMBOLS.has(quoteTok.symbol)) main = quoteTok;
        if (!main) continue;
        if (!binanceFutures.has(main.symbol)) continue;

        allTokens.push({ ...main, network, pool: poolAddr, h24Vol });
      }
    }
  }

  // Deduplicate — keep highest-volume pool per token address
  const tokenMap = new Map();
  for (const t of allTokens) {
    const ex = tokenMap.get(t.address);
    if (!ex || t.h24Vol > ex.h24Vol) tokenMap.set(t.address, t);
  }

  const sorted = [...tokenMap.values()].sort((a, b) => b.h24Vol - a.h24Vol);
  const slice  = sorted.slice(FROM_RANK, TO_RANK);
  const forKV  = slice.map(({ h24Vol, ...rest }) => rest);

  // Safety guard: a failed Binance/GeckoTerminal fetch (e.g. Binance blocking the
  // Cloudflare Workers egress IP) can silently zero out binanceFutures, which would
  // otherwise wipe the whole token list. Never overwrite with a suspiciously small result.
  const MIN_EXPECTED_TOKENS = Math.min(10, TO_RANK - FROM_RANK);
  if (forKV.length < MIN_EXPECTED_TOKENS) {
    console.error(`updateTokenList ABORTED: only ${forKV.length} tokens found (binanceFutures=${binanceFutures.size} symbols) — keeping existing KV list`);
    return;
  }

  await RANK_STORE.put(KV_TOKEN_LIST, JSON.stringify(forKV));
  console.log(`updateTokenList done: ${forKV.length} tokens (indices ${FROM_RANK}–${FROM_RANK + forKV.length - 1})`);
}

// ============================================================
// Entry point
// ============================================================

// Temporary diagnostic trace (2026-07-13) — writes a checkpoint to KV every few
// tokens so a stuck/cut-off run can be inspected via KV directly, since wrangler
// tail has proven unreliable (connection failures) for capturing scheduled-event
// logs in this environment. Remove once the free-tier reversion is confirmed stable.
async function handleScheduled(event, env) {
  const { RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY } = env;

  if (!TELEGRAM_BOT_TOKEN) throw new Error('Missing TELEGRAM_BOT_TOKEN');
  if (!TELEGRAM_CHAT_ID)   throw new Error('Missing TELEGRAM_CHAT_ID');
  if (!COINGECKO_API_KEY)  throw new Error('Missing COINGECKO_API_KEY');

  const tokens = await getTokenList(RANK_STORE);
  if (!tokens) {
    console.warn('Token list empty — run scripts/update-token-list.js to populate KV');
    return;
  }

  const withPool = tokens.filter(t => t.pool);
  console.log(`Scanning ${withPool.length} tokens with pools (${tokens.length - withPool.length} skipped — no pool)...`);

  const debug = { cron: event.cron, startedAt: Date.now(), tokensTotal: withPool.length, processed: 0, errors: [] };
  await RANK_STORE.put('debug:lastRun', JSON.stringify(debug));

  for (let i = 0; i < withPool.length; i++) {
    try {
      await processToken(withPool[i], RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY);
      debug.processed++;
    } catch (err) {
      debug.errors.push({ i, symbol: withPool[i].symbol, message: String(err.message).slice(0, 200) });
      console.error(`${withPool[i].symbol} [${withPool[i].network}]: ${err.message}`);
    }
    if (i % 5 === 0 || i === withPool.length - 1) {
      debug.lastCheckpointAt = Date.now();
      debug.lastIndex = i;
      await RANK_STORE.put('debug:lastRun', JSON.stringify(debug));
    }
    if (i < withPool.length - 1) await sleep(GECKO_DELAY_MS);
  }

  debug.completedAt = Date.now();
  await RANK_STORE.put('debug:lastRun', JSON.stringify(debug));
  console.log(`Done. ${withPool.length} tokens scanned.`);
}

export default {
  async scheduled(event, env, ctx) {
    if (event.cron === '0 3 * * 7') {
      ctx.waitUntil(updateTokenList(env));
    } else if (!USE_PAID_API && event.cron === '* * * * *') {
      // Free tier: main scan runs on its own slower cron (20 min); this fast
      // per-minute tick only checks existing pending/open signals for confirmation.
      const { RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY } = env;
      ctx.waitUntil(checkPendingAlerts(RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY));
      ctx.waitUntil(checkOpenSignals(RANK_STORE, COINGECKO_API_KEY));
      ctx.waitUntil(checkScannerHeartbeat(RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID));
    } else if (!USE_PAID_API) {
      // Free tier: this is the slower main-scan cron (*/20 or staggered).
      ctx.waitUntil(handleScheduled(event, env));
    } else {
      // Paid tier: a single per-minute cron does everything.
      const { RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY } = env;
      ctx.waitUntil(handleScheduled(event, env));
      ctx.waitUntil(checkPendingAlerts(RANK_STORE, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, COINGECKO_API_KEY));
      ctx.waitUntil(checkOpenSignals(RANK_STORE, COINGECKO_API_KEY));
    }
  },

  async fetch(req, env, ctx) {
    const { pathname } = new URL(req.url);
    if (pathname === '/trigger' && req.method === 'POST') {
      ctx.waitUntil(handleScheduled({ cron: 'manual' }, env));
      return new Response('Triggered', { status: 200 });
    }
    if (pathname === '/ping' && req.method === 'POST') {
      const { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } = env;
      const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: 'Ping from worker — secrets OK' }),
      });
      const json = await res.json();
      return new Response(JSON.stringify(json), { status: res.status, headers: { 'Content-Type': 'application/json' } });
    }
    // Diagnostic: which upstream APIs are reachable from this worker's egress IP.
    // Added 2026-07-16 while debugging frozen confirmations/resolutions.
    if (pathname === '/pricetest' && req.method === 'GET') {
      const out = {};
      try {
        const r = await fetch(`${COINGECKO_BASE}/simple/price?ids=bitcoin&vs_currencies=usd`, { headers: UA_HEADER, cf: { cacheTtl: 0 } });
        out.coingecko = { status: r.status, body: (await r.text()).slice(0, 150) };
      } catch (e) { out.coingecko = { error: e.message }; }
      // the real production path used by confirmations/resolutions:
      out.fetchTokenPrice = await fetchTokenPrice('bitcoin', null, null, null).catch(e => ({ error: e.message }));
      // does our custom User-Agent actually survive the Workers fetch?
      try {
        const r = await fetch('https://httpbin.org/headers', { headers: UA_HEADER });
        out.echoedHeaders = (await r.json()).headers;
      } catch (e) { out.echoedHeaders = { error: e.message }; }
      try {
        const r = await fetch(`${GECKO_BASE}/networks/eth/tokens/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2`, { headers: { Accept: 'application/json;version=20230302' }, cf: { cacheTtl: 0 } });
        out.geckoterminal = { status: r.status };
      } catch (e) { out.geckoterminal = { error: e.message }; }
      try {
        const r = await fetch('https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT', { cf: { cacheTtl: 0 } });
        out.binance = { status: r.status, body: (await r.text()).slice(0, 100) };
      } catch (e) { out.binance = { error: e.message }; }
      return new Response(JSON.stringify(out, null, 2), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    // On-request analytics over v2 signals (epoch 2026-07-16): win rate by
    // entry condition (viaClass), chain, alert type, and the full SL/TP grid
    // replayed from each signal's price trail — answers "which stop/take is
    // best" and "which pending path has the highest win rate".
    if (pathname === '/analytics' && req.method === 'GET') {
      const { RANK_STORE } = env;
      // ?all=1 ignores the current stats epoch and grids over ALL v2 history —
      // use it for a statistically meaningful SL/TP read; default is epoch-only.
      const gate = new URL(req.url).searchParams.get('all') === '1' ? 0 : STATS_START_TS;
      const records = [];
      for (const prefix of ['siglog:win:', 'siglog:loss:', 'siglog:flat:']) {
        let result = await RANK_STORE.list({ prefix, limit: 1000 });
        while (true) {
          for (const { name } of result.keys) {
            const raw = await RANK_STORE.get(name);
            if (!raw) continue;
            const s = JSON.parse(raw);
            if (s.v === 2 && s.ts >= gate) {
              s.typeSide = `${s.alertType}:${s.side}`;
              records.push(s);
            }
          }
          if (result.list_complete) break;
          result = await RANK_STORE.list({ prefix, limit: 1000, cursor: result.cursor });
        }
      }

      // Replay the whole SL/TP grid over an arbitrary record subset, best
      // expectancy first. Reused for the global grid and each per-network grid.
      const computeGrid = recs => {
        const g = [];
        for (const sl of SL_GRID) {
          for (const tp of TP_GRID) {
            const key = `${sl}/${tp}`;
            let wins = 0, losses = 0;
            for (const r of recs) {
              const o = r.grid?.[key];
              if (o === 'win') wins++;
              else if (o === 'loss') losses++;
            }
            const n = wins + losses;
            g.push({
              slPct: sl, tpPct: tp, n, wins, losses,
              winRatePct: n ? +(wins / n * 100).toFixed(1) : null,
              // R-нормалізація: стоп = 1R, тейк = tp/sl R
              expectancyR: n ? +(((wins * tp / sl) - losses) / n).toFixed(2) : null,
            });
          }
        }
        g.sort((a, b) => (b.expectancyR ?? -99) - (a.expectancyR ?? -99));
        return g;
      };
      const bySLTP = computeGrid(records);

      // Per-network grid + best combo (min sample so tiny chains don't mislead).
      const networks = [...new Set(records.map(r => r.network))];
      const gridByNetwork = {};
      for (const net of networks) {
        const recs = records.filter(r => r.network === net);
        const grid = computeGrid(recs);
        gridByNetwork[net] = {
          n: recs.length,
          bestCombo: grid.find(c => c.n >= 10) ?? null, // null → замало даних для висновку
          fullGrid: grid,
        };
      }

      const groupBy = field => {
        const out = {};
        for (const r of records) {
          const k = r[field] ?? '?';
          out[k] = out[k] ?? { n: 0, wins: 0, losses: 0, flat: 0 };
          out[k].n++;
          if (r.status === 'win') out[k].wins++;
          else if (r.status === 'loss') out[k].losses++;
          else out[k].flat++;
        }
        for (const k of Object.keys(out)) {
          const g = out[k];
          const d = g.wins + g.losses;
          g.winRatePct = d ? +(g.wins / d * 100).toFixed(1) : null;
        }
        return out;
      };

      let openTracking = 0;
      let result = await RANK_STORE.list({ prefix: 'siglog:open:', limit: 1000 });
      while (true) {
        openTracking += result.keys.length;
        if (result.list_complete) break;
        result = await RANK_STORE.list({ prefix: 'siglog:open:', limit: 1000, cursor: result.cursor });
      }

      const body = {
        epoch: new Date(STATS_START_TS).toISOString(),
        note: `Групові win rate — по headline ${SIGLOG_SL_PCT}/${SIGLOG_TP_PCT}; flat = headline не зачепило за ${MAX_TRACK_H}г/±${WIDE_PCT}%. Грід SL/TP реплеїться з цінового трейлу кожного сигналу; gridByNetwork.bestCombo = null означає <10 закритих сигналів у мережі.`,
        closedSignals: records.length,
        openTracking,
        bestCombos: bySLTP.filter(c => c.n >= 10).slice(0, 5),
        byVia: groupBy('viaClass'),
        byNetwork: groupBy('network'),
        byTypeSide: groupBy('typeSide'),
        gridByNetwork,
        fullGrid: bySLTP,
      };
      return new Response(JSON.stringify(body, null, 2), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    // Temporary (2026-07-19): send one sample of each chain/type/side to Telegram
    // so the user can see the redesigned alert rendering. Remove after review.
    if (pathname === '/preview' && req.method === 'POST') {
      const { TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID } = env;
      const mk = (addr, rank, prevRank, vol, txs) => ({ address: addr, rank, prevRank, volume_usd: vol, txs: new Array(txs).fill(vol / txs), paymentTokenAddr: null, lastTxHash: null });
      const price = { usd: 0.0234, usd_24h_change: 12.4 };
      const samples = [
        ['bsc',    mk('0x1234567890abcdef1234567890abcdef12345678', 2, 14, 42000, 5), 'buy',  'activity', 'm5 +6.2%'],
        ['base',   mk('0xabcdef1234567890abcdef1234567890abcdef12', 3, 18, 31000, 1), 'sell', 'jump',     'h24 ▲ #4 +19.1%'],
        ['eth',    mk('0x9876543210fedcba9876543210fedcba98765432', 6, 9,  27000, 1), 'buy',  'stepup',   'm30 ▲ #7 +3.4%'],
        ['solana', mk('FkaLnX17cXabcDEfGhiJkLmNoPqRsTuVwXyZ123456', 4, 21, 15000, 3), 'sell', 'activity', 'm30 ▼ #12 -4.8%'],
      ];
      for (const [net, jump, side, type, via] of samples) {
        await sendAlert(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, jump, 'TESTTOKEN', net, '0x0000000000000000000000000000000000000001', side, type, price, via, {});
      }
      return new Response(`sent ${samples.length} preview alerts`, { status: 200 });
    }
    if (pathname === '/stats' && req.method === 'GET') {
      const { RANK_STORE } = env;
      const raw = await RANK_STORE.get(STATS_KEY);
      const stats = raw ? JSON.parse(raw) : { wins: 0, losses: 0, totalR: 0 };

      let openCount = 0;
      const openTrades = [];
      let result = await RANK_STORE.list({ prefix: 'siglog:open:', limit: 100 });
      while (true) {
        for (const { name } of result.keys) {
          const s = JSON.parse(await RANK_STORE.get(name));
          openCount++; openTrades.push(s);
        }
        if (result.list_complete) break;
        result = await RANK_STORE.list({ prefix: 'siglog:open:', limit: 100, cursor: result.cursor });
      }

      const resolved = stats.wins + stats.losses;
      const body = {
        wins: stats.wins,
        losses: stats.losses,
        winRatePct: resolved ? +(stats.wins / resolved * 100).toFixed(1) : null,
        totalR: stats.totalR,
        expectancyR: resolved ? +(stats.totalR / resolved).toFixed(2) : null,
        openCount,
        lastUpdated: stats.lastUpdated ?? null,
        openTrades,
      };
      return new Response(JSON.stringify(body, null, 2), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    return new Response('gecko-rank-alert v2', { status: 200 });
  },
};
