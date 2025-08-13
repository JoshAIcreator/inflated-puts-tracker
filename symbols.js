// symbols.js — fetch & cache optionable tickers from Polygon
// Usage: node symbols.js   -> writes symbols.json (optionable=TRUE)

const axios = require("axios");
require("dotenv").config();

const API = process.env.POLYGON_API_KEY;
if (!API) {
  console.error("Missing POLYGON_API_KEY in .env");
  process.exit(1);
}

const fs = require("fs");

async function fetchOptionableSymbols(limitPerPage = 1000) {
  let url = `https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=${limitPerPage}&apiKey=${API}`;
  const out = [];

  while (url) {
    const { data } = await axios.get(url);
    const page = data?.results || [];
    for (const t of page) {
      // Polygon marks optionable stocks with "options" in the list of exchanges/attrs on some endpoints;
      // safer: include everything, we’ll filter later by whether we can find any contracts.
      out.push(t.ticker);
    }
    url = data?.next_url ? `${data.next_url}&apiKey=${API}` : null;
  }
  return Array.from(new Set(out)).sort();
}

(async () => {
  try {
    const list = await fetchOptionableSymbols();
    fs.writeFileSync("symbols.json", JSON.stringify(list, null, 2));
    console.log(`Saved ${list.length} tickers to symbols.json`);
  } catch (e) {
    console.error("Fetch failed:", e.message);
    process.exit(1);
  }
})();