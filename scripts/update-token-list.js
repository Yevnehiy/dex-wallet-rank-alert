#!/usr/bin/env node
// ============================================================
// Update token list in Cloudflare KV
// NEW: uses GeckoTerminal top pools by 24h DEX volume
// Usage:
//   node scripts/update-token-list.js                        (indices 0-70, uses .env)
//   node scripts/update-token-list.js --from 70 --to 140 --env .env.2
//
// Requires scripts/.env with:
//   CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, KV_NAMESPACE_ID
// ============================================================

const https = require('https');
const fs    = require('fs');
const path  = require('path');

// ---- Parse CLI args ----
const args   = process.argv.slice(2);
const getArg = (name, def) => { const i = args.indexOf(name); return i !== -1 ? args[i + 1] : def; };
const FROM_RANK = parseInt(getArg('--from', '0'));
const TO_RANK   = parseInt(getArg('--to',   '70'));
const ENV_FILE  = getArg('--env', '.env');

if (isNaN(FROM_RANK) || isNaN(TO_RANK) || FROM_RANK >= TO_RANK) {
  console.error('Invalid range. Usage: --from 0 --to 70'); process.exit(1);
}

// ---- Load .env ----
const envPath = path.join(__dirname, ENV_FILE);
if (!fs.existsSync(envPath)) { console.error(`Env file not found: ${envPath}`); process.exit(1); }
fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
  const [k, ...v] = line.split('=');
  if (k && v.length) process.env[k.trim()] = v.join('=').trim();
});

const CF_ACCOUNT_ID   = process.env.CLOUDFLARE_ACCOUNT_ID;
const CF_API_TOKEN    = process.env.CLOUDFLARE_API_TOKEN;
const KV_NAMESPACE_ID = process.env.KV_NAMESPACE_ID;
const COINGECKO_API_KEY = process.env.COINGECKO_API_KEY;
if (!CF_ACCOUNT_ID || !CF_API_TOKEN || !KV_NAMESPACE_ID) {
  console.error(`Missing env vars in ${ENV_FILE}`); process.exit(1);
}
if (!COINGECKO_API_KEY) {
  console.error(`Missing COINGECKO_API_KEY in ${ENV_FILE}`); process.exit(1);
}

// ---- Constants ----
const GECKO_BASE      = 'https://pro-api.coingecko.com/api/v3/onchain';
const GECKO_DELAY_MS  = 500; // paid plan: 250 req/min budget
const PAGES_PER_NET   = 10;  // 20 pools/page × 10 pages × 7 networks = 1400 pool entries
const MIN_POOL_VOL    = 50000; // skip pools with <$50k 24h volume

const NETWORKS = ['eth', 'base', 'bsc']; // arbitrum/polygon_pos/optimism/avax dropped 2026-07-15 — negligible DEX-volume share, cut for API budget

const STABLECOIN_SYMBOLS = new Set([
  'USDT','USDC','DAI','BUSD','TUSD','FRAX','FDUSD','PYUSD','EURC','EUTBL',
  'GHO','USDG','USDY','OUSG','RLUSD','USDM','STABLE','USTB','USAT','USDA',
  'USDAI','USDTB','USD0','AUSD','REUSD','CRVUSD','SATUSD','EURCV','USDF',
  'PAXG','XAUT','KAU','KAG','APXUSD','USDD','EURSAFO','JAAA','JTRSY',
  'EARNETH','M','U','BORG',
  // Additional stablecoins and yield-bearing USD derivatives:
  'USDS','USDE','SUSDE','SDAI','USDX','GUSDC','LISUSD','EURE','EUROE',
]);

// Tokens that are the "boring" side of a trading pair
const SKIP_SYMBOLS = new Set([
  ...STABLECOIN_SYMBOLS,
  'WETH','ETH','WBTC','BTC','WBNB','BNB','WMATIC','MATIC','WAVAX','AVAX',
  'WSTETH','STETH','CBETH','RETH','WBETH','CBBTC','WEETH','EZETH','RSETH',
]);

// ---- HTTP helpers ----
function get(url, headers = {}) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'rank-alert/1.0', ...headers } }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString();
        if (res.statusCode !== 200) reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
        else resolve(JSON.parse(body));
      });
    }).on('error', reject);
  });
}

function cfPut(kvKey, value) {
  return new Promise((resolve, reject) => {
    const body = Buffer.from(JSON.stringify(value));
    const options = {
      hostname: 'api.cloudflare.com',
      path:     `/client/v4/accounts/${CF_ACCOUNT_ID}/storage/kv/namespaces/${KV_NAMESPACE_ID}/values/${encodeURIComponent(kvKey)}`,
      method:   'PUT',
      headers: { Authorization: `Bearer ${CF_API_TOKEN}`, 'Content-Type': 'application/json', 'Content-Length': body.length },
    };
    const req = https.request(options, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const data = JSON.parse(Buffer.concat(chunks).toString());
        if (!data.success) reject(new Error(`CF KV put failed: ${JSON.stringify(data.errors)}`));
        else resolve(data);
      });
    });
    req.on('error', reject);
    req.write(body); req.end();
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---- Binance Futures filter ----
async function getBinanceFuturesSymbols() {
  const res = await get('https://fapi.binance.com/fapi/v1/exchangeInfo');
  const symbols = new Set();
  for (const s of res.symbols) {
    if (s.contractType !== 'PERPETUAL' || s.quoteAsset !== 'USDT' || s.status !== 'TRADING') continue;
    const base = s.baseAsset.toUpperCase();
    symbols.add(base);
    // Handle 1000PEPE → PEPE, 1000000MOG → MOG
    const stripped = base.replace(/^\d+/, '');
    if (stripped && stripped !== base) symbols.add(stripped);
  }
  console.log(`Binance Futures USDT-PERP: ${symbols.size} unique symbols (incl. 1000X stripped)`);
  return symbols;
}

// ---- Main ----
async function main() {
  console.log(`DEX volume range: indices ${FROM_RANK}–${TO_RANK - 1} (env: ${ENV_FILE})`);

  // Load Binance Futures whitelist first (fast, single request)
  const binanceFutures = await getBinanceFuturesSymbols();

  console.log(`Fetching top pools from GeckoTerminal (${NETWORKS.length} networks × ${PAGES_PER_NET} pages)...\n`);

  const allTokens = [];
  let reqCount = 0;

  for (const network of NETWORKS) {
    for (let page = 1; page <= PAGES_PER_NET; page++) {
      if (reqCount > 0) await sleep(GECKO_DELAY_MS);

      let res;
      try {
        res = await get(
          `${GECKO_BASE}/networks/${network}/pools?sort=h24_volume_usd_desc&page=${page}&include=base_token,quote_token`,
          { Accept: 'application/json;version=20230302', 'x-cg-pro-api-key': COINGECKO_API_KEY }
        );
      } catch (e) {
        console.error(`  ${network} p${page}: ${e.message}`);
        reqCount++;
        continue;
      }
      reqCount++;

      // Build token lookup from included data
      const tokenLookup = new Map();
      for (const item of (res.included ?? [])) {
        if (item.type !== 'token') continue;
        tokenLookup.set(item.id, {
          address: item.attributes.address?.toLowerCase(),
          symbol:  item.attributes.symbol?.toUpperCase() ?? '?',
          id:      item.attributes.coingecko_coin_id ?? null,
        });
      }

      let added = 0;
      for (const pool of (res.data ?? [])) {
        const poolAddr = pool.attributes?.address?.toLowerCase();
        const h24Vol   = parseFloat(pool.attributes?.volume_usd?.h24 ?? 0);
        if (!poolAddr || h24Vol < MIN_POOL_VOL) continue;

        const baseId   = pool.relationships?.base_token?.data?.id;
        const quoteId  = pool.relationships?.quote_token?.data?.id;
        const baseTok  = tokenLookup.get(baseId);
        const quoteTok = tokenLookup.get(quoteId);

        // Pick the non-boring side of the pair
        let main = null;
        if (baseTok && baseTok.address && !SKIP_SYMBOLS.has(baseTok.symbol)) main = baseTok;
        else if (quoteTok && quoteTok.address && !SKIP_SYMBOLS.has(quoteTok.symbol)) main = quoteTok;
        if (!main) continue;

        // Only keep tokens listed on Binance Futures USDT-PERP
        if (!binanceFutures.has(main.symbol)) continue;

        allTokens.push({ ...main, network, pool: poolAddr, h24Vol });
        added++;
      }
      console.log(`  ${network} p${page}: ${res.data?.length ?? 0} pools, ${added} tokens added`);
    }
  }

  // Deduplicate — keep highest-volume pool per token address
  const tokenMap = new Map();
  for (const t of allTokens) {
    const ex = tokenMap.get(t.address);
    if (!ex || t.h24Vol > ex.h24Vol) tokenMap.set(t.address, t);
  }

  // Sort by 24h volume desc
  const sorted = [...tokenMap.values()].sort((a, b) => b.h24Vol - a.h24Vol);
  console.log(`\nUnique tokens before slice: ${sorted.length}`);

  // Slice to our range
  const slice = sorted.slice(FROM_RANK, TO_RANK);

  // Remove h24Vol before saving (not needed in worker)
  const forKV = slice.map(({ h24Vol, ...rest }) => rest);

  const MIN_EXPECTED_TOKENS = Math.min(10, TO_RANK - FROM_RANK);
  if (forKV.length < MIN_EXPECTED_TOKENS) {
    console.error(`ABORTED: only ${forKV.length} tokens found — refusing to overwrite KV list. Check Binance API / GeckoTerminal fetch above.`);
    process.exit(1);
  }

  console.log(`\nSaving ${forKV.length} tokens to KV...`);
  await cfPut('tokens', forKV);

  console.log(`Done! Indices ${FROM_RANK}–${FROM_RANK + forKV.length - 1} → KV key "tokens"`);
  console.log(`\nTop 10 by 24h DEX volume:`);
  slice.slice(0, 10).forEach((t, i) =>
    console.log(`  ${FROM_RANK + i + 1}. ${t.symbol.padEnd(10)} ${t.network.padEnd(12)} $${(t.h24Vol / 1e6).toFixed(1)}M  pool=${t.pool.slice(0, 10)}...`)
  );
  const dist = {};
  forKV.forEach(t => { dist[t.network] = (dist[t.network] || 0) + 1; });
  console.log('Chain distribution:', dist);
  const withId = forKV.filter(t => t.id).length;
  console.log(`CoinGecko ID available: ${withId}/${forKV.length}`);
}

main().catch(err => { console.error('Error:', err.message); process.exit(1); });
