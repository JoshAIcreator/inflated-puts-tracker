// index.js — Fat Put Finder (CLI)
// Usage: node index.js ATYR,HIMS,MBX
// Pulls Polygon option quotes, estimates theoretical put price (B-S), and flags
// near-the-money puts where Bid >> Theoretical price.

import axios from "axios";
import "dotenv/config";

const API = process.env.POLYGON_API_KEY;
if (!API) {
  console.error("Missing POLYGON_API_KEY in .env");
  process.exit(1);
}

// ---- tiny math helpers (Black–Scholes) ----
function cdf(x) {
  const a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741, a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
  const sign = x < 0 ? -1 : 1;
  const z = Math.abs(x) / Math.SQRT2;
  const t = 1 / (1 + p * z);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-z * z);
  return 0.5 * (1 + sign * y);
}
function bsPutTheo({ S, K, T, r = 0.05, sigma }) {
  if (T <= 0 || sigma <= 0) return Math.max(K - S, 0);
  const d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
  const d2 = d1 - sigma * Math.sqrt(T);
  return K * Math.exp(-r * T) * cdf(-d2) - S * cdf(-d1);
}

// ---- simple helpers ----
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const daysBetween = (a, b) => Math.max(0, Math.ceil((b - a) / 86400000));

// ---- Polygon adapters ----
// 1) Get current underlying price
async function getUnderlyingPrice(ticker) {
  // use last trade
  const url = `https://api.polygon.io/v2/last/trade/${encodeURIComponent(ticker)}?apiKey=${API}`;
  const { data } = await axios.get(url);
  if (!data?.results?.p) throw new Error(`No last trade for ${ticker}`);
  return data.results.p;
}

// 2) List option contracts by underlying (nearest expirations, both calls & puts)
async function listContracts(ticker, limit = 200) {
  // Polygon reference contracts by underlying
  const url = `https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=${encodeURIComponent(ticker)}&limit=${limit}&apiKey=${API}`;
  const { data } = await axios.get(url);
  return data?.results ?? [];
}

// 3) Get NBBO quote for a specific option symbol
async function getOptionQuote(optionSymbol) {
  const url = `https://api.polygon.io/v3/quotes/${encodeURIComponent(optionSymbol)}?limit=1&apiKey=${API}`;
  const { data } = await axios.get(url);
  const q = data?.results?.[0];
  if (!q) return null;
  const bid = q?.bid_price ?? null;
  const ask = q?.ask_price ?? null;
  const iv = q?.greeks?.iv ?? null; // may be null depending on plan/data
  return { bid, ask, iv };
}

// ---- main scan ----
async function scanTicker(ticker) {
  try {
    const S = await getUnderlyingPrice(ticker);
    const all = await listContracts(ticker, 500);

    // focus: near-the-money PUTS, expirations within ~120 days
    const now = new Date();
    const candidates = all
      .filter(c =>
        c.exercise_style === "american" &&
        c.contract_type === "put" &&
        c.expiration_date &&
        daysBetween(now, new Date(c.expiration_date)) <= 120 &&
        Math.abs((c.strike_price ?? 0) - S) / S <= 0.30 // within 30% of spot
      )
      .slice(0, 150); // keep it light

    const rows = [];
    for (const c of candidates) {
      // gentle pacing to avoid rate-limits
      await sleep(50);
      const quote = await getOptionQuote(c.ticker);
      if (!quote || quote.bid == null || quote.ask == null) continue;

      const mid = (quote.bid + quote.ask) / 2;
      const K = c.strike_price;
      const dte = daysBetween(now, new Date(c.expiration_date));
      const T = dte / 365;

      // use IV from quote if available; else fallback to conservative sigma
      const sigma = quote.iv ? Math.max(0.05, Math.min(6, quote.iv)) : 1.0; // 100% fallback

      const theo = bsPutTheo({ S, K, T, sigma });
      const bidTheo = theo > 0 ? quote.bid / theo : Infinity;

      rows.push({
        option: c.ticker,
        expiry: c.expiration_date,
        strike: K,
        bid: Number(quote.bid.toFixed(2)),
        ask: Number(quote.ask.toFixed(2)),
        mid: Number(mid.toFixed(2)),
        spot: Number(S.toFixed(2)),
        dte,
        iv: quote.iv ? Number((quote.iv * 100).toFixed(0)) : null,
        theo: Number(theo.toFixed(2)),
        bidTheo: Number(bidTheo.toFixed(2)),
      });
    }

    // sort by “fatness” (Bid/Theo), show top 15
    rows.sort((a, b) => b.bidTheo - a.bidTheo);
    return rows.slice(0, 15);
  } catch (e) {
    console.error(`[${ticker}] scan failed:`, e.message);
    return [];
  }
}

async function main() {
  const tickers = (process.argv[2] ?? "ATYR").split(",").map(s => s.trim()).filter(Boolean);
  console.log(`Scanning: ${tickers.join(", ")}\n`);

  const all = [];
  for (const t of tickers) {
    const rows = await scanTicker(t);
    if (rows.length) {
      console.log(`=== ${t} ===`);
      console.table(rows);
      all.push(...rows.map(r => ({ ...r, underlying: t })));
    } else {
      console.log(`=== ${t} ===\n(no juicy puts found)\n`);
    }
  }
}

main();
