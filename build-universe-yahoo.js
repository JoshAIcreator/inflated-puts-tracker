// build-universe-yahoo.js
// Pulls *optionable* U.S. equities from Yahoo’s screener (free) and writes universe.txt
// If Yahoo throttles, re-run; the script paginates until it exhausts results.

const fs = require("fs");
const axios = require("axios");

const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36";

// Yahoo Screener POST endpoint
const URL = "https://query2.finance.yahoo.com/v1/finance/screener";

async function fetchPage(offset = 0, size = 250) {
  // Query: U.S. region, quoteType=EQUITY, optionable=true, active
  const body = {
    offset,
    size,
    sortField: "symbol",
    sortType: "asc",
    quoteType: "EQUITY",
    query: {
      operator: "and",
      operands: [
        { operator: "eq", operands: ["region", "us"] },
        { operator: "eq", operands: ["optionable", true] },
        { operator: "eq", operands: ["isYahooFinance", true] },
      ],
    },
  };

  const { data } = await axios.post(URL, body, {
    headers: {
      "Content-Type": "application/json",
      "User-Agent": UA,
      "Accept": "application/json, text/plain, */*",
      "Origin": "https://finance.yahoo.com",
      "Referer": "https://finance.yahoo.com/screener/",
    },
    // be polite
    timeout: 15000,
    validateStatus: s => s >= 200 && s < 500,
  });

  if (!data || !data.finance || !data.finance.result || !data.finance.result[0]) {
    throw new Error(`Unexpected response at offset ${offset}`);
  }
  const res = data.finance.result[0];
  const quotes = res.quotes || [];
  const tickers = quotes.map(q => q.symbol).filter(Boolean);
  const total = res.total ?? tickers.length;

  return { tickers, total };
}

(async () => {
  const seen = new Set();
  let offset = 0;
  const size = 250;
  let total = 999999;

  console.log("Building optionable universe from Yahoo…");
  while (offset < total) {
    try {
      const { tickers, total: t } = await fetchPage(offset, size);
      total = t ?? total;
      tickers.forEach(tk => seen.add(tk));
      console.log(`Fetched ${tickers.length} (offset ${offset}) — unique so far: ${seen.size}/${total}`);
      offset += size;

      // small delay to avoid throttle
      await new Promise(r => setTimeout(r, 300));
      if (tickers.length === 0) break; // guard if Yahoo stops returning pages
    } catch (e) {
      console.warn(`Page at offset ${offset} failed: ${e.message}. Retrying in 5s…`);
      await new Promise(r => setTimeout(r, 5000));
    }
  }

  const list = Array.from(seen).sort();
  fs.writeFileSync("universe.txt", list.join("\n"));
  console.log(`\nSaved ${list.length} tickers to universe.txt`);
})();