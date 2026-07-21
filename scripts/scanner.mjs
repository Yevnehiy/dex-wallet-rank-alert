#!/usr/bin/env node
// ============================================================
// Local detection scanner — runs on the user's PC because GeckoTerminal
// blocks Cloudflare Workers egress IPs (verified 2026-07-15).
//
// Every 5 min:
//   1. Binance Futures leaderboards (2 requests, all USDT-perps):
//      h24 top25 gainers/losers, m30 top25 gainers/losers (from our own
//      price snapshots), m5 movers |Δ|>=3%.
//   2. Dedup: one token is checked via exactly ONE condition,
//      priority m5 → m30 → h24 (fastest timeframe wins).
//   3. For each selected token with a known DEX pool: fetch trades from
//      GeckoTerminal (free tier, home IP), run the same jump/stepup/activity
//      wallet detection as the worker, push `pending:` candidates to KV.
//   4. The Cloudflare worker keeps confirming/resolving/alerting 24/7.
//
// Usage:
//   node scripts/scanner.mjs            continuous 5-min loop
//   node scripts/scanner.mjs --once     single cycle
//   node scripts/scanner.mjs --dry      no KV writes (state file still saved,
//                                       so a second --dry run gets m5 deltas)
// ============================================================

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  fetchTrades, extractRefPrice,
  aggregateTopBuyers, aggregateTopSellers,
  detectJumps, detectStepUp, detectActivity,
  pendingKey, cooldownKey, normAddr,
  SKIP_SYMBOLS, PENDING_TTL_S,
} from '../worker/index.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ---- Config ----
const CYCLE_MS          = 5 * 60 * 1000;
const BOARD_SIZE        = 25;      // top-N per side per timeframe
const M5_THRESHOLD_PCT  = 5;       // |Δ| over 5 min that makes a token a "mover"
                                   // (3→5 on 2026-07-18: at 3% the m5 path won only
                                   // 27% — late tops; entry must be a real anomaly)
const M5_CAP            = 30;      // market-crash guard: cap movers by |Δ|
// GT free tier is documented at 30 req/min but measured (2026-07-15, home IP)
// at ~8-9 sustained req/min: ~5-request burst allowance, then 429 until the
// window refills. 6.5s pacing stays just under the refill rate.
const GT_DELAY_MS       = 6500;
const GT_DELAY_MAX_MS   = 15000;
const SCAN_CAP          = 25;      // max tokens scanned per cycle — dedup order is
                                   // priority order (m5 → m30 → h24), so the cap
                                   // drops the slowest board's tail first.
                                   // 25×6.5s keeps the cycle ≈5 min; at 40 the cycle
                                   // stretched to 9-10 min and the m5 board never
                                   // populated (price snapshot older than tolerance)
const POOLMAP_TTL_MS    = 24 * 3600 * 1000;
const POOLMAP_NETWORKS  = ['eth', 'base', 'bsc', 'solana'];

// "Boring" quote side on Solana pairs only — SOL must NOT go into the global
// SKIP_SYMBOLS: bridged SOL on Base is a tracked token (our top signal producer)
// and has to stay the "interesting" side of EVM pairs.
const SOL_QUOTE_SYMBOLS = new Set(['SOL', 'WSOL', 'MSOL', 'JITOSOL', 'JUPSOL', 'BNSOL', 'BONKSOL', 'LST']);
const POOLMAP_PAGES     = 10;      // 20 pools/page
const MIN_POOL_VOL      = 50000;
const HIST_MIN_AGE_MS   = 55 * 60 * 1000;  // rank snapshot age window for jump/stepup
const HIST_MAX_AGE_MS   = 75 * 60 * 1000;  // (worker compares vs exactly 60 min ago)
const SNAP_KEEP_MS      = 80 * 60 * 1000;
const STATE_FILE        = path.join(__dirname, 'scanner-state.json');
const LOG_FILE          = path.join(__dirname, 'scanner.log');

const args = process.argv.slice(2);
const ONCE = args.includes('--once');
const DRY  = args.includes('--dry');

// ---- .env (account 1) ----
const envPath = path.join(__dirname, '.env');
for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
  const [k, ...v] = line.split('=');
  if (k && v.length) process.env[k.trim()] = v.join('=').trim();
}
const CF_ACCOUNT_ID = process.env.CLOUDFLARE_ACCOUNT_ID;
const CF_API_TOKEN  = process.env.CLOUDFLARE_API_TOKEN;
const KV_NS         = process.env.KV_NAMESPACE_ID;
if (!CF_ACCOUNT_ID || !CF_API_TOKEN || !KV_NS) {
  console.error('Missing CLOUDFLARE_ACCOUNT_ID / CLOUDFLARE_API_TOKEN / KV_NAMESPACE_ID in scripts/.env');
  process.exit(1);
}
const KV_BASE = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${KV_NS}`;

// ---- Helpers ----
const sleep = ms => new Promise(r => setTimeout(r, ms));

function log(msg) {
  const line = `${new Date().toISOString()} ${msg}`;
  console.log(line);
  try {
    if (fs.existsSync(LOG_FILE) && fs.statSync(LOG_FILE).size > 5 * 1024 * 1024) {
      fs.renameSync(LOG_FILE, LOG_FILE + '.old');
    }
    fs.appendFileSync(LOG_FILE, line + '\n', 'utf8');
  } catch { /* logging must never kill the scanner */ }
}

async function fetchJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url.split('/').slice(-1)[0].split('?')[0]}: HTTP ${res.status}`);
  return res.json();
}

async function kvGet(key) {
  const res = await fetch(`${KV_BASE}/values/${encodeURIComponent(key)}`, {
    headers: { Authorization: `Bearer ${CF_API_TOKEN}` },
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`KV get ${key}: HTTP ${res.status}`);
  return res.text();
}

async function kvPut(key, value, ttlS) {
  const url = `${KV_BASE}/values/${encodeURIComponent(key)}` + (ttlS ? `?expiration_ttl=${ttlS}` : '');
  const res = await fetch(url, {
    method: 'PUT',
    headers: { Authorization: `Bearer ${CF_API_TOKEN}` },
    body: value,
  });
  if (!res.ok) throw new Error(`KV put ${key}: HTTP ${res.status}`);
}

async function kvListKeys(prefix) {
  const names = [];
  let cursor = '';
  do {
    const url = `${KV_BASE}/keys?prefix=${encodeURIComponent(prefix)}&limit=1000` + (cursor ? `&cursor=${cursor}` : '');
    const res = await fetch(url, { headers: { Authorization: `Bearer ${CF_API_TOKEN}` } });
    if (!res.ok) throw new Error(`KV list ${prefix}: HTTP ${res.status}`);
    const data = await res.json();
    names.push(...data.result.map(k => k.name));
    cursor = data.result_info?.cursor ?? '';
  } while (cursor);
  return names;
}

// ---- State (disk, tolerant) ----
function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    return { priceRing: [], rankSnaps: {}, ...s };
  } catch {
    return { priceRing: [], rankSnaps: {} };
  }
}

function saveState(state) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state), 'utf8');
  } catch (e) {
    log(`state save failed: ${e.message}`);
  }
}

// ---- Binance Futures universe: Map<perpSymbol, baseSymbol> ----
// baseSymbol has the 1000/1000000 multiplier stripped (1000PEPEUSDT → PEPE);
// deltas are % so the multiplier cancels out.
async function getFuturesUniverse(state) {
  if (state.futures && Date.now() - state.futures.updatedAt < 24 * 3600 * 1000) {
    return new Map(state.futures.entries);
  }
  try {
    const data = await fetchJson('https://fapi.binance.com/fapi/v1/exchangeInfo');
    const entries = [];
    for (const s of data.symbols) {
      if (s.contractType !== 'PERPETUAL' || s.quoteAsset !== 'USDT' || s.status !== 'TRADING') continue;
      const base = s.baseAsset.toUpperCase();
      entries.push([s.symbol, base.replace(/^\d+/, '') || base]);
    }
    if (entries.length < 50) throw new Error(`suspiciously few perps: ${entries.length}`);
    state.futures = { updatedAt: Date.now(), entries };
    return new Map(entries);
  } catch (e) {
    if (state.futures) {
      log(`exchangeInfo failed (${e.message}) — using cached list`);
      return new Map(state.futures.entries);
    }
    throw e;
  }
}

// ---- symbol → best DEX pool map (refreshed daily via GeckoTerminal) ----
async function refreshPoolMap(state, futuresBases) {
  if (state.poolMap && Date.now() - state.poolMap.updatedAt < POOLMAP_TTL_MS) {
    return state.poolMap.map;
  }
  log(`pool map refresh: ${POOLMAP_NETWORKS.length} networks x ${POOLMAP_PAGES} pages...`);
  const found = [];
  let pageFails = 0;
  for (const network of POOLMAP_NETWORKS) {
    for (let page = 1; page <= POOLMAP_PAGES; page++) {
      await sleep(gtDelay);
      let data;
      try {
        // one retry on 429, same pattern as fetchTrades
        for (let attempt = 0; ; attempt++) {
          const res = await fetch(
            `https://api.geckoterminal.com/api/v2/networks/${network}/pools?sort=h24_volume_usd_desc&page=${page}&include=base_token,quote_token`,
            { headers: { Accept: 'application/json;version=20230302' } }
          );
          if (res.status === 429 && attempt === 0) { await sleep(7000); continue; }
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          data = await res.json();
          break;
        }
      } catch (e) {
        log(`  poolmap ${network} p${page}: ${e.message}`);
        pageFails++;
        continue;
      }

      const tokenLookup = new Map();
      for (const item of (data.included ?? [])) {
        if (item.type !== 'token') continue;
        tokenLookup.set(item.id, {
          address: normAddr(item.attributes.address), // base58 is case-sensitive — only 0x gets lowercased
          symbol:  item.attributes.symbol?.toUpperCase() ?? '?',
          id:      item.attributes.coingecko_coin_id ?? null,
        });
      }
      for (const pool of (data.data ?? [])) {
        const poolAddr = normAddr(pool.attributes?.address);
        const h24Vol   = parseFloat(pool.attributes?.volume_usd?.h24 ?? 0);
        if (!poolAddr || h24Vol < MIN_POOL_VOL) continue;
        const baseTok  = tokenLookup.get(pool.relationships?.base_token?.data?.id);
        const quoteTok = tokenLookup.get(pool.relationships?.quote_token?.data?.id);
        const boring   = sym => SKIP_SYMBOLS.has(sym) || (network === 'solana' && SOL_QUOTE_SYMBOLS.has(sym));
        let main = null;
        if (baseTok?.address && !boring(baseTok.symbol)) main = baseTok;
        else if (quoteTok?.address && !boring(quoteTok.symbol)) main = quoteTok;
        if (!main || !futuresBases.has(main.symbol)) continue;
        found.push({ ...main, network, pool: poolAddr, h24Vol });
      }
    }
  }
  // one best (highest-volume) pool per base symbol
  const fresh = {};
  for (const t of found) {
    if (!fresh[t.symbol] || t.h24Vol > fresh[t.symbol].h24Vol) fresh[t.symbol] = t;
  }
  // Merge over the previous map: a partial refresh (429'd pages) must not
  // erase known pools — stale-but-present beats missing.
  const map = { ...(state.poolMap?.map ?? {}), ...fresh };
  if (Object.keys(map).length < 10) {
    log(`poolmap ABORT: only ${Object.keys(map).length} tokens — keeping previous map`);
    return state.poolMap?.map ?? {};
  }
  // Partial refresh → backdate so it retries in ~1h instead of waiting 24h.
  const partial = pageFails > POOLMAP_NETWORKS.length * POOLMAP_PAGES * 0.2;
  state.poolMap = {
    updatedAt: partial ? Date.now() - POOLMAP_TTL_MS + 3600 * 1000 : Date.now(),
    map,
  };
  log(`pool map: ${Object.keys(map).length} tokens (${Object.keys(fresh).length} fresh${partial ? `, PARTIAL ${pageFails} pages failed — retry in ~1h` : ''})`);
  return map;
}

// ---- On-demand pool search for board tokens missing from the crawl map ----
// Solana's top-pools pages are polluted by wash-traded scam pools (homoglyph
// "USDT" pairs faking $B volumes) that push legit pools below the crawl cutoff,
// so tail tokens are resolved individually via GT search. Positive hits go into
// poolMap; misses are negative-cached for 24h (state.searchMiss).
async function searchPool(base, state) {
  await sleep(gtDelay);
  let data;
  try {
    const res = await fetch(
      `https://api.geckoterminal.com/api/v2/search/pools?query=${encodeURIComponent(base)}&include=base_token,quote_token`,
      { headers: { Accept: 'application/json;version=20230302' } }
    );
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    data = await res.json();
  } catch (e) {
    log(`  search ${base}: ${e.message}`);
    return null; // transient — don't negative-cache
  }
  const tokenLookup = new Map();
  for (const item of (data.included ?? [])) {
    if (item.type !== 'token') continue;
    tokenLookup.set(item.id, {
      address: normAddr(item.attributes.address),
      symbol:  (item.attributes.symbol ?? '').toUpperCase().replace(/^\$/, ''), // '$WIF' → 'WIF'
      id:      item.attributes.coingecko_coin_id ?? null,
    });
  }
  let best = null;
  for (const pool of (data.data ?? [])) {
    // pool id is "<network>_<address>"; only our tracked networks qualify
    const network = pool.id?.split('_')[0];
    if (!POOLMAP_NETWORKS.includes(network)) continue;
    const h24Vol = parseFloat(pool.attributes?.volume_usd?.h24 ?? 0);
    if (h24Vol < MIN_POOL_VOL) continue;
    const baseTok  = tokenLookup.get(pool.relationships?.base_token?.data?.id);
    const quoteTok = tokenLookup.get(pool.relationships?.quote_token?.data?.id);
    const match = [baseTok, quoteTok].find(t => t?.symbol === base && t?.address);
    if (!match) continue;
    const poolAddr = normAddr(pool.attributes?.address);
    if (!poolAddr) continue;
    if (!best || h24Vol > best.h24Vol) best = { address: match.address, symbol: base, id: match.id, network, pool: poolAddr, h24Vol };
  }
  state.searchMiss = state.searchMiss ?? {};
  if (best) {
    state.poolMap.map[base] = best;
    delete state.searchMiss[base];
    log(`  search ${base}: found ${best.network} pool, vol $${Math.round(best.h24Vol / 1000)}k`);
  } else {
    state.searchMiss[base] = Date.now();
  }
  return best;
}

// ---- Leaderboards from Binance Futures prices ----
async function buildBoards(state, universe) {
  const [t24, tp] = await Promise.all([
    fetchJson('https://fapi.binance.com/fapi/v1/ticker/24hr'),
    fetchJson('https://fapi.binance.com/fapi/v1/ticker/price'),
  ]);
  const now = Date.now();

  const prices = {};
  for (const p of tp) if (universe.has(p.symbol)) prices[p.symbol] = parseFloat(p.price);

  state.priceRing = (state.priceRing ?? []).filter(s => now - s.ts < 40 * 60 * 1000);
  state.priceRing.push({ ts: now, prices });

  const h24 = [];
  for (const t of t24) {
    if (!universe.has(t.symbol)) continue;
    const pct = parseFloat(t.priceChangePercent);
    if (!isNaN(pct)) h24.push({ symbol: t.symbol, pct });
  }

  // Δ% vs our own snapshot closest to targetAge (null until warm-up)
  const deltas = (targetAgeMs, tolMs) => {
    let best = null;
    for (const s of state.priceRing.slice(0, -1)) {
      if (!best || Math.abs(now - s.ts - targetAgeMs) < Math.abs(now - best.ts - targetAgeMs)) best = s;
    }
    if (!best || Math.abs(now - best.ts - targetAgeMs) > tolMs) return null;
    const out = [];
    for (const [sym, price] of Object.entries(prices)) {
      const old = best.prices[sym];
      if (old > 0) out.push({ symbol: sym, pct: (price - old) / old * 100 });
    }
    return out;
  };
  const d5  = deltas(5 * 60 * 1000, 2.5 * 60 * 1000);
  const d30 = deltas(30 * 60 * 1000, 6 * 60 * 1000);

  // FULL sorted lists (no slice) — board quotas are applied during selection so
  // CEX-only tokens don't waste board slots: if #15 has no DEX pool, #26 takes
  // its place, walking as deep as needed (user rule, 2026-07-16).
  const top = (arr, dir) => arr
    .filter(x => dir > 0 ? x.pct > 0 : x.pct < 0)
    .sort((a, b) => dir * (b.pct - a.pct));

  return {
    m5:  d5 ? d5.filter(x => Math.abs(x.pct) >= M5_THRESHOLD_PCT)
              .sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct)) : [],
    g30: d30 ? top(d30, +1) : [],
    l30: d30 ? top(d30, -1) : [],
    g24: top(h24, +1),
    l24: top(h24, -1),
  };
}

// Selection: one token is checked via exactly ONE condition (fastest timeframe
// claims first), each board fills its quota with SCANNABLE tokens only —
// tokens without a DEX pool are skipped and the next-ranked one backfills.
// `via` keeps the token's TRUE rank in the full board (e.g. "h24 ▲ #31").
async function selectScanSet(boards, universe, poolMap, state) {
  const claimed = new Set();
  const scanSet = [];
  let noPool = 0;
  let searchesLeft = 10; // spread pool discovery over cycles, don't blow the budget
  for (const [label, viaClass, list, quota] of [
    ['m5',    'm5',    boards.m5,  M5_CAP],
    ['m30 ▲', 'm30▲',  boards.g30, BOARD_SIZE],
    ['m30 ▼', 'm30▼',  boards.l30, BOARD_SIZE],
    ['h24 ▲', 'h24▲',  boards.g24, BOARD_SIZE],
    ['h24 ▼', 'h24▼',  boards.l24, BOARD_SIZE],
  ]) {
    let taken = 0;
    for (let i = 0; i < list.length && taken < quota; i++) {
      const t = list[i];
      const base = universe.get(t.symbol);
      if (!base || claimed.has(base)) continue;
      let pool = poolMap[base];
      if (!pool) {
        const missAt = state.searchMiss?.[base];
        const cachedMiss = missAt && Date.now() - missAt < 24 * 3600 * 1000;
        if (!cachedMiss && searchesLeft > 0) {
          searchesLeft--;
          pool = await searchPool(base, state);
        }
      }
      if (!pool) { noPool++; continue; } // CEX-only/unresolved — next-ranked token backfills
      claimed.add(base);
      taken++;
      const rank = label === 'm5' ? '' : `#${i + 1} `;
      scanSet.push({ base, pool, viaClass, via: `${label} ${rank}${t.pct >= 0 ? '+' : ''}${t.pct.toFixed(1)}%` });
    }
  }
  return { scanSet, noPool };
}

// ---- Per-token wallet detection (same logic as worker's processToken) ----
const slim = traders => traders.map(({ address, rank, volume_usd }) => ({ address, rank, volume_usd }));

// Anti-wash filter (user rule 2026-07-18): a wallet that both bought AND sold
// the same token within the window is painting volume, not expressing intent —
// drop its candidates when the opposite-side volume is meaningful. Computed
// from ALL trades in the window (not just the top-20 aggregation) so a #1
// seller hiding at #25 on the buy side is still caught.
const WASH_OPPOSITE_RATIO = 0.25; // opposite side ≥25% of the signal side = wash

function sideVolumes(trades, kind) {
  const since = Date.now() - 60 * 60 * 1000; // same 60-min window as aggregation
  const map = new Map();
  for (const t of trades) {
    const a = t.attributes;
    if (a.kind !== kind) continue;
    if (new Date(a.block_timestamp).getTime() < since) continue;
    const addr = normAddr(a.tx_from_address);
    if (!addr) continue;
    map.set(addr, (map.get(addr) ?? 0) + (parseFloat(a.volume_in_usd) || 0));
  }
  return map;
}

async function scanToken(entry, state) {
  const p = entry.pool; // {address, symbol, id, network, pool}
  const rawTrades = await fetchTrades(p.pool, p.network, null);
  if (rawTrades.length === 0) return { candidates: 0, written: 0, washed: 0, addr: p.address, priceUsd: null };

  const refPrice = extractRefPrice(rawTrades, p.address);
  const buyers   = aggregateTopBuyers(rawTrades);
  const sellers  = aggregateTopSellers(rawTrades);

  const now   = Date.now();
  const snaps = state.rankSnaps[p.address] ?? [];
  const hist  = snaps.filter(s => {
    const age = now - s.ts;
    return age >= HIST_MIN_AGE_MS && age <= HIST_MAX_AGE_MS;
  }).pop() ?? null;

  snaps.push({ ts: now, buyers: slim(buyers), sellers: slim(sellers) });
  state.rankSnaps[p.address] = snaps.filter(s => now - s.ts < SNAP_KEEP_MS);

  const cands = [];
  for (const [side, current, histSide] of [['buy', buyers, hist?.buyers], ['sell', sellers, hist?.sellers]]) {
    for (const j of detectJumps(current, histSide ?? null))  cands.push([side, 'jump', j]);
    for (const j of detectStepUp(current, histSide ?? null)) cands.push([side, 'stepup', j]);
    for (const j of detectActivity(current))                 cands.push([side, 'activity', j]);
  }

  const buyVol  = sideVolumes(rawTrades, 'buy');
  const sellVol = sideVolumes(rawTrades, 'sell');

  let written = 0, washed = 0;
  for (const [side, alertType, jump] of cands) {
    const opposite = (side === 'buy' ? sellVol : buyVol).get(jump.address) ?? 0;
    if (opposite >= jump.volume_usd * WASH_OPPOSITE_RATIO) {
      washed++;
      log(`  wash-skip: ${p.symbol} ${side} ${jump.address.slice(0, 10)} signal=$${jump.volume_usd.toFixed(0)} opposite=$${opposite.toFixed(0)}`);
      continue;
    }
    log(`  candidate: ${p.symbol} ${side} ${alertType} wallet=${jump.address.slice(0, 10)} vol=$${jump.volume_usd.toFixed(0)} (${entry.via})`);
    if (DRY) continue;
    if (await kvGet(pendingKey(side, alertType, p.address, jump.address))) continue;
    if (await kvGet(cooldownKey(side, alertType, p.address, jump.address))) continue;
    await kvPut(pendingKey(side, alertType, p.address, jump.address), JSON.stringify({
      side, alertType,
      tokenAddr: p.address, tokenId: p.id, tokenSymbol: p.symbol, network: p.network,
      jump,
      referencePrice: refPrice?.usd ?? null,
      via: entry.via,
      viaClass: entry.viaClass, // stable group key for analytics (via has rank/pct in it)
    }), PENDING_TTL_S);
    written++;
  }
  return { candidates: cands.length, written, washed, addr: p.address, priceUsd: refPrice?.usd ?? null };
}

// ---- Price relay ----
// Since ~2026-07-15 CoinGecko AND GeckoTerminal are 429-blocked from Cloudflare's
// egress IPs at cron volumes, so the worker can't fetch prices to confirm pendings
// or resolve SL/TP. The scanner (home IP) publishes DEX prices for every token that
// has a pending: or siglog:open: record — from this cycle's own trades (free) plus
// GT's batched token_price endpoint (30 addresses/request) — and batched CG market
// caps for pending tokenIds. The worker reads these maps instead of fetching.
async function publishPrices(state, cyclePrices) {
  // Spot USD prices of major quote assets (from this cycle's Binance tickers) —
  // the worker converts alert balances (WBNB/WETH/SOL/…) into dollars with these.
  const latest = state.priceRing?.[state.priceRing.length - 1]?.prices ?? {};
  const quotes = {};
  for (const [sym, perp] of [['BTC', 'BTCUSDT'], ['ETH', 'ETHUSDT'], ['BNB', 'BNBUSDT'], ['SOL', 'SOLUSDT'], ['AVAX', 'AVAXUSDT'], ['POL', 'POLUSDT']]) {
    if (latest[perp] > 0) quotes[sym] = latest[perp];
  }
  if (Object.keys(quotes).length) {
    await kvPut('quotes:usd', JSON.stringify(quotes)).catch(e => log(`quotes: ${e.message}`));
  }

  const openKeys = await kvListKeys('siglog:open:');
  const pendKeys = await kvListKeys('pending:');

  // read each record once: addr -> {network, tokenId}
  const recs = {};
  const pendIds = new Set();
  for (const k of [...openKeys, ...pendKeys]) {
    const isPend = k.startsWith('pending:');
    const addr = isPend ? k.split(':')[3] : k.split(':')[2];
    if (!recs[addr]) {
      try {
        const raw = await kvGet(k);
        if (!raw) continue;
        const r = JSON.parse(raw);
        recs[addr] = { network: r.network, tokenId: r.tokenId ?? null };
      } catch { continue; }
    }
    if (isPend && recs[addr].tokenId) pendIds.add(recs[addr].tokenId);
  }

  // batch-fetch missing prices from GT, grouped by network, 30 addrs/request
  const prices = { ...cyclePrices };
  const byNet = {};
  for (const [addr, r] of Object.entries(recs)) {
    if (prices[addr] || !r.network) continue;
    (byNet[r.network] = byNet[r.network] ?? []).push(addr);
  }
  for (const [net, addrs] of Object.entries(byNet)) {
    for (let i = 0; i < addrs.length; i += 30) {
      await sleep(gtDelay);
      try {
        const chunk = addrs.slice(i, i + 30);
        const res = await fetch(
          `https://api.geckoterminal.com/api/v2/simple/networks/${net}/token_price/${chunk.map(encodeURIComponent).join(',')}`,
          { headers: { Accept: 'application/json;version=20230302' } }
        );
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        const ts = Date.now();
        for (const [addr, p] of Object.entries(data.data?.attributes?.token_prices ?? {})) {
          const usd = parseFloat(p);
          if (usd > 0) prices[addr] = { usd, ts };
        }
      } catch (e) { log(`  token_price ${net}: ${e.message}`); }
    }
  }
  await kvPut('prices:dex', JSON.stringify(prices));

  // batched market caps for pending tokenIds — single CG request from home IP
  if (pendIds.size) {
    try {
      const res = await fetch(
        `https://api.coingecko.com/api/v3/simple/price?ids=${[...pendIds].join(',')}&vs_currencies=usd&include_market_cap=true`,
        { headers: { 'User-Agent': 'rank-alert/1.0' } }
      );
      if (res.ok) {
        const data = await res.json();
        const ts = Date.now();
        const mcaps = {};
        for (const [id, v] of Object.entries(data)) {
          if (v.usd_market_cap) mcaps[id] = { mcap: v.usd_market_cap, ts };
        }
        await kvPut('mcaps:cg', JSON.stringify(mcaps));
      } else {
        log(`  cg mcap batch: HTTP ${res.status}`);
      }
    } catch (e) { log(`  cg mcap batch: ${e.message}`); }
  }
  return Object.keys(prices).length;
}

// ---- Cycle ----
let gtDelay = GT_DELAY_MS;

async function cycle(state) {
  const t0 = Date.now();
  const universe = await getFuturesUniverse(state);
  const poolMap  = await refreshPoolMap(state, new Set(universe.values()));
  const boards   = await buildBoards(state, universe);
  const { scanSet: scannable, noPool } = await selectScanSet(boards, universe, poolMap, state);
  const dropped = scannable.splice(SCAN_CAP); // over-cap tail = lowest-priority boards

  let ok = 0, fail = 0, rateLimited = 0, candidates = 0, written = 0, washTotal = 0;
  const cyclePrices = {}; // addr -> {usd, ts} from this cycle's own trades — free
  for (const e of scannable) {
    // PC sleep/hibernate mid-cycle: on wake the loop would keep scanning with
    // hours-old board data. Abandon the cycle — the next one rebuilds boards.
    if (Date.now() - t0 > 15 * 60 * 1000) {
      log(`  boards stale (cycle >15 min — PC slept?) — abandoning rest of cycle`);
      break;
    }
    try {
      const r = await scanToken(e, state);
      ok++; candidates += r.candidates; written += r.written; washTotal += r.washed;
      if (r.priceUsd) cyclePrices[r.addr] = { usd: r.priceUsd, ts: Date.now() };
    } catch (err) {
      fail++;
      if (/rate limit/i.test(err.message)) rateLimited++;
      else log(`  ${e.base}: ${err.message}`);
    }
    await sleep(gtDelay);
  }

  let relayedCount = 0;
  if (!DRY) {
    try { relayedCount = await publishPrices(state, cyclePrices); }
    catch (e) { log(`price relay: ${e.message}`); }
  }

  // Adaptive pacing: sustained 429s → slow down next cycle, recover gradually.
  if (rateLimited >= 5) gtDelay = Math.min(GT_DELAY_MAX_MS, Math.round(gtDelay * 1.5));
  else gtDelay = Math.max(GT_DELAY_MS, Math.round(gtDelay * 0.8));

  if (!DRY) await kvPut('heartbeat:scanner', String(Date.now())).catch(e => log(`heartbeat: ${e.message}`));
  saveState(state);

  log(
    `cycle: m5=${boards.m5.length} m30=${boards.g30.length}▲/${boards.l30.length}▼ h24=${boards.g24.length}▲/${boards.l24.length}▼ (full lists)` +
    ` | scan=${scannable.length} capped=${dropped.length} noPoolWalked=${noPool} ok=${ok} fail=${fail} (429:${rateLimited})` +
    ` | cand=${candidates} wash=${washTotal} pending=${written} relay=${relayedCount}` +
    ` | ${Math.round((Date.now() - t0) / 1000)}s delay=${gtDelay}ms` +
    (DRY ? ' [DRY]' : '')
  );
}

// ---- Main loop ----
(async () => {
  log(`scanner start (once=${ONCE} dry=${DRY})`);
  const state = loadState();
  for (;;) {
    const started = Date.now();
    try {
      await cycle(state);
    } catch (e) {
      log(`CYCLE ERROR: ${e.message}`);
      saveState(state);
    }
    if (ONCE) break;
    await sleep(Math.max(5000, CYCLE_MS - (Date.now() - started)));
  }
})();
