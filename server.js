// server.js — web UI + API wrapper for the Fat Put Finder (Yahoo, ~15m delayed) + Live progress (SSE)

const express = require("express");
const fs = require("fs");
const path = require("path");
const yahooFinance = require("yahoo-finance2").default;

yahooFinance.suppressNotices(["yahooSurvey"]);
yahooFinance.setGlobalConfig({ validation: { logErrors: false } });

const app = express();
const PORT = 3000;

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

/* ---------- Math helpers ---------- */
function cdf(x){ const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=0.3275911;
  const s=x<0?-1:1,z=Math.abs(x)/Math.SQRT2,t=1/(1+p*z);
  const y=1-(((((a5*t+a4)*t+a3)*t+a2)*t+a1)*t)*Math.exp(-z*z);
  return 0.5*(1+s*y);
}
function bsPutTheo({S,K,T,r=0.05,sigma}){
  if(T<=0||sigma<=0) return Math.max(K-S,0);
  const d1=(Math.log(S/K)+(r+0.5*sigma*sigma)*T)/(sigma*Math.sqrt(T));
  const d2=d1-sigma*Math.sqrt(T);
  return K*Math.exp(-r*T)*cdf(-d2)-S*cdf(-d1);
}
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const daysBetween = (a,b) => Math.max(0, Math.ceil((b-a)/86400000));

function readUniverse(file){
  const raw = fs.readFileSync(path.resolve(file), "utf8");
  return raw.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
}

// Helper: Robustly pick a spot price from a Yahoo quote object
function pickSpotFromQuote(q) {
  const candidates = [
    q?.regularMarketPrice,
    q?.regularMarketPreviousClose,
    q?.previousClose,
    q?.postMarketPrice,
    q?.preMarketPrice
  ];
  for (const v of candidates) {
    if (Number.isFinite(v) && v > 0) return Number(v);
  }
  return null;
}

/* ---------- core scan for one ticker ---------- */
// Debug stats structure:
// { ticker, expiries, totalPuts, nearKept, pricedKept, minIntrinsicKept, volOIKept, hits, notes: [] }
async function scanOne(ticker, opts){
  const {
    maxDTE=120, near=0.30, maxExp=3,
    minBidTheo=1.20, minVol=0, minOI=0,
    sleepMs=120, mode="regular",
    intrinsic=true, intrinsicOnly=false, minIntrinsic=0
  } = opts;

  const returnStats = !!opts.returnStats;
  const stats = { ticker, expiries: 0, totalPuts: 0, nearKept: 0, pricedKept: 0, minIntrinsicKept: 0, volOIKept: 0, hits: 0, notes: [] };

  try {
    const head = await yahooFinance.options(ticker);

    // Try to extract a usable spot from the quote that comes with the options response
    let spot = pickSpotFromQuote(head?.quote || {});

    // On weekends / after-hours Yahoo sometimes omits prices from the options payload.
    // Fall back to a direct quote call to retrieve previousClose/regular prices.
    if (!Number.isFinite(spot) || spot <= 0) {
      try {
        const q2 = await yahooFinance.quote(ticker);
        spot = pickSpotFromQuote(q2 || {});
      } catch {
        // ignore
      }
    }

    // As an absolute last resort in after-hours mode, try to use previousClose from the summary detail API
    if (!Number.isFinite(spot) || spot <= 0) {
      if (mode === "afterhours") {
        try {
          const qs = await yahooFinance.quoteSummary(ticker, { modules: ["price", "summaryDetail"] });
          const p = qs?.price || {};
          const sd = qs?.summaryDetail || {};
          spot = pickSpotFromQuote({
            regularMarketPrice: p?.regularMarketPrice?.raw,
            regularMarketPreviousClose: p?.regularMarketPreviousClose?.raw,
            previousClose: sd?.previousClose?.raw
          });
        } catch {
          // ignore
        }
      }
    }

    if (!Number.isFinite(spot) || spot <= 0) {
      return returnStats ? { hits: [], stats } : [];
    }

    const expiries = (head?.expirationDates||[])
      .map(Number).filter(Boolean).slice(0, maxExp);

    stats.expiries = expiries.length;

    const now = Date.now();
    const hits = [];

    for (const epoch of expiries) {
      const dte = daysBetween(now, epoch*1000);
      if (dte > maxDTE) continue;

      await sleep(sleepMs);
      const chain = await yahooFinance.options(ticker, { date: epoch });
      const opt = chain?.options?.[0];
      if (!opt?.puts?.length) continue;

      stats.totalPuts += (opt.puts?.length || 0);

      for (const p of opt.puts) {
        const K = Number(p.strike);
        if (!K) continue;

        if (Math.abs(K-spot)/spot > near) { if (returnStats && stats.notes.length < 10) stats.notes.push(`skip by near: K=${K}, spot=${spot}`); continue; }
        stats.nearKept++;

        let bid = Number(p.bid ?? 0);
        let ask = Number(p.ask ?? 0);
        let mid = ((bid||0)+(ask||0))/2 || bid || ask;

        const intrinsicVal = Math.max(K - spot, 0);
        const itmPct = +(spot ? ((K-spot)/spot)*100 : 0).toFixed(2);

        let used = mid;
        let priceUsed = "mid";

        if (mode === "afterhours" && intrinsicOnly) {
          used = intrinsicVal;
          priceUsed = "intrinsic";
        } else if (mode === "afterhours") {
          const last = Number(p.lastPrice ?? 0);
          if (!isFinite(used) || used<=0) {
            if (last>0) { used = last; priceUsed = "last"; }
            else if (intrinsic && intrinsicVal>0) { used = intrinsicVal; priceUsed = "intrinsic"; }
          }
        }

        if (!isFinite(used) || used <= 0) { if (returnStats && stats.notes.length < 10) stats.notes.push(`skip no price usable (mid/last/intrinsic)`); continue; }
        stats.pricedKept++;

        let iv = p.impliedVolatility; if (iv==null) iv = 1.0; if (iv>10) iv/=100;
        iv = Math.min(6, Math.max(0.05, iv));
        const T = dte/365;
        const theo = bsPutTheo({S:spot,K,T,sigma:iv});
        const bidTheo = theo>0 ? (used/theo) : Infinity;

        const vol = p.volume ?? 0, oi = p.openInterest ?? 0;

        if (mode === "afterhours" && minIntrinsic>0 && intrinsicVal < minIntrinsic) { if (returnStats && stats.notes.length < 10) stats.notes.push(`skip by minIntrinsic ${intrinsicVal} < ${minIntrinsic}`); continue; }
        stats.minIntrinsicKept++;

        if (bidTheo < minBidTheo) continue;

        if (vol < minVol || oi < minOI) { if (returnStats && stats.notes.length < 10) stats.notes.push(`skip by vol/oi vol=${vol} oi=${oi}`); continue; }
        stats.volOIKept++;

        stats.hits++;

        hits.push({
          ticker,
          option: p.contractSymbol,
          expiry: new Date(epoch*1000).toISOString().slice(0,10),
          strike: K,
          bid: Number((p.bid ?? 0).toFixed?.(2) ?? 0),
          ask: Number((p.ask ?? 0).toFixed?.(2) ?? 0),
          usedPrice: Number(used.toFixed(2)),
          priceUsed, // mid | last | intrinsic
          spot: Number(spot.toFixed(2)),
          intrinsic: Number(intrinsicVal.toFixed(2)),
          intrinsicGap: Number((used - intrinsicVal).toFixed(2)),
          itmPct,
          dte,
          ivPct: Number((iv*100).toFixed(0)),
          theo: Number(theo.toFixed(2)),
          bidTheo: Number(bidTheo.toFixed(2)),
          volume: vol, oi
        });
      }
    }
    if (returnStats) {
      return { hits, stats };
    }
    return hits;
  } catch {
    return returnStats ? { hits: [], stats } : [];
  }
}

/* ---------- existing JSON + CSV endpoints (unchanged) ---------- */
app.get("/api/scan", async (req, res) => {
  res.setHeader("Content-Type", "application/json");

  const {
    tickers,
    minBidTheo="1.2", minVol="0", minOI="0",
    maxDTE="120", near="0.30", exp="3",
    concurrency="5", limit="50", sleepMs="120",
    mode="regular",
    intrinsic="1", intrinsicOnly="0", minIntrinsic="0",
    debug="0"
  } = req.query;

  const opts = {
    minBidTheo: Number(minBidTheo),
    minVol: Number(minVol),
    minOI: Number(minOI),
    maxDTE: Number(maxDTE),
    near: Number(near),
    maxExp: Number(exp),
    sleepMs: Number(sleepMs),
    mode: (mode === "afterhours" ? "afterhours" : "regular"),
    intrinsic: intrinsic === "1" || intrinsic === "true",
    intrinsicOnly: intrinsicOnly === "1" || intrinsicOnly === "true",
    minIntrinsic: Number(minIntrinsic) || 0,
    debug: (debug === "1" || debug === "true")
  };

  let universe = [];
  if (tickers && String(tickers).trim().length) {
    universe = String(tickers).split(",").map(s=>s.trim()).filter(Boolean);
  } else {
    universe = readUniverse("universe.txt");
  }

  const total = universe.length;
  const maxConc = Math.max(1, Math.min(Number(concurrency)||5, 16));
  const hardLimit = Math.max(1, Math.min(Number(limit)||50, 5000));

  let idx = 0;
  const allHits = [];

  async function worker(){
    while (idx < total) {
      const me = idx++;
      const t = universe[me];
      const hits = await scanOne(t, opts);
      if (hits.length) allHits.push(...hits);
    }
  }
  await Promise.all(Array.from({length: maxConc}, worker));

  allHits.sort((a,b)=> b.bidTheo - a.bidTheo);
  const out = allHits.slice(0, hardLimit);

  res.end(JSON.stringify({
    mode: opts.mode,
    totalTickers: total,
    returned: out.length,
    filters: opts,
    results: out
  }));
});

app.get("/api/diagnose", async (req, res) => {
  res.setHeader("Content-Type", "application/json");
  const {
    tickers="AAPL",
    minBidTheo="0.1", minVol="0", minOI="0",
    maxDTE="180", near="1.20", exp="5",
    sleepMs="50", mode="afterhours",
    intrinsic="1", intrinsicOnly="0", minIntrinsic="0"
  } = req.query;

  const opts = {
    minBidTheo: Number(minBidTheo),
    minVol: Number(minVol),
    minOI: Number(minOI),
    maxDTE: Number(maxDTE),
    near: Number(near),
    maxExp: Number(exp),
    sleepMs: Number(sleepMs),
    mode: (mode === "afterhours" ? "afterhours" : "regular"),
    intrinsic: intrinsic === "1" || intrinsic === "true",
    intrinsicOnly: intrinsicOnly === "1" || intrinsicOnly === "true",
    minIntrinsic: Number(minIntrinsic) || 0,
    returnStats: true
  };

  const list = String(tickers).split(",").map(s=>s.trim()).filter(Boolean);
  const out = [];
  for (const t of list) {
    const r = await scanOne(t, opts);
    out.push({ ticker: t, stats: r.stats, sampleHits: r.hits.slice(0,5) });
  }
  res.end(JSON.stringify({ diagnose: out }));
});

app.get("/api/scan.csv", async (req, res) => {
  const url = new URL(req.protocol + "://" + req.get("host") + req.originalUrl);
  url.pathname = "/api/scan";
  const resp = await fetch(url.toString());
  const data = await resp.json();
  const rows = data.results || [];
  res.setHeader("Content-Disposition", "attachment; filename=\"hits.csv\"");
  res.setHeader("Content-Type", "text/csv");
  if (!rows.length) {
    return res.send("ticker,option,expiry,strike,bid,ask,usedPrice,priceUsed,spot,intrinsic,intrinsicGap,itmPct,dte,ivPct,theo,bidTheo,volume,oi\n");
  }
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")].concat(rows.map(r => headers.map(h => r[h]).join(",")));
  res.send(lines.join("\n"));
});

/* ---------- NEW: Live progress via Server-Sent Events ---------- */
app.get("/api/scan_sse", async (req, res) => {
  // SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // prevent proxy buffering
  res.flushHeaders?.();
  // Suggest client retry interval
  res.write(`retry: 5000\n\n`);
  // Heartbeat to keep the connection alive
  const heartbeat = setInterval(() => {
    res.write(`: ping ${Date.now()}\n\n`);
  }, 15000);
  // Clean up if the client disconnects
  req.on("close", () => {
    clearInterval(heartbeat);
    try { res.end(); } catch {}
  });

  const send = (event, payload) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  };

  // Parse query
  const {
    tickers,
    minBidTheo="1.2", minVol="0", minOI="0",
    maxDTE="120", near="0.30", exp="3",
    concurrency="5", limit="50", sleepMs="120",
    mode="regular",
    intrinsic="1", intrinsicOnly="0", minIntrinsic="0",
    debug="0"
  } = req.query;

  const opts = {
    minBidTheo: Number(minBidTheo),
    minVol: Number(minVol),
    minOI: Number(minOI),
    maxDTE: Number(maxDTE),
    near: Number(near),
    maxExp: Number(exp),
    sleepMs: Number(sleepMs),
    mode: (mode === "afterhours" ? "afterhours" : "regular"),
    intrinsic: intrinsic === "1" || intrinsic === "true",
    intrinsicOnly: intrinsicOnly === "1" || intrinsicOnly === "true",
    minIntrinsic: Number(minIntrinsic) || 0,
    debug: (debug === "1" || debug === "true")
  };

  let universe = [];
  if (tickers && String(tickers).trim().length) {
    universe = String(tickers).split(",").map(s=>s.trim()).filter(Boolean);
  } else {
    try { universe = readUniverse("universe.txt"); }
    catch { universe = []; }
  }

  const total = universe.length || 0;
  const maxConc = Math.max(1, Math.min(Number(concurrency)||5, 16));
  const hardLimit = Math.max(1, Math.min(Number(limit)||50, 5000));

  send("start", { total, filters: opts });

  let idx = 0, done = 0, lastPing = Date.now();
  const allHits = [];

  async function worker(){
    while (idx < total) {
      const me = idx++;
      const t = universe[me];
      const hits = await scanOne(t, opts);
      if (hits.length) {
        allHits.push(...hits);
        send("hits", { ticker: t, count: hits.length, totalHits: allHits.length });
      }
      done++;
      const now = Date.now();
      if (now - lastPing > 500) {
        lastPing = now;
        send("progress", { done, total, pct: total ? +(done/total*100).toFixed(1) : 100 });
      }
    }
  }

  if (total === 0) {
    send("done", { total, returned: 0, results: [] });
    return res.end();
  }

  await Promise.all(Array.from({length: maxConc}, worker));

  allHits.sort((a,b)=> b.bidTheo - a.bidTheo);
  const out = allHits.slice(0, hardLimit);

  send("done", { total, returned: out.length, results: out });
  clearInterval(heartbeat);
  res.end();
});

// Basic health check
app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: Date.now() });
});
// List available GET routes for quick diagnostics
app.get("/routes", (_req, res) => {
  const list = (app._router?.stack || [])
    .filter(r => r.route && r.route.methods && r.route.methods.get)
    .map(r => r.route.path);
  res.json({ routes: list });
});

/* ---------- start server ---------- */
app.listen(PORT, () => {
  console.log(`Fat Put Finder UI running → http://localhost:${PORT}`);
});