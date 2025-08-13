// universe helpers (pure module — no Express references here)
const fs = require("fs");
const path = require("path");

function readLines(absPath) {
  try {
    return fs.readFileSync(absPath, "utf8")
      .split(/\r?\n/)
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);
  } catch (_) {
    return [];
  }
}

function loadUniverseFromFile() {
  const baseDir = process.cwd();
  // 1) explicit env override
  if (process.env.UNIVERSE_FILE && process.env.UNIVERSE_FILE.trim()) {
    const p = path.resolve(baseDir, process.env.UNIVERSE_FILE.trim());
    const list = readLines(p);
    console.log(`[universe] using ${p} (${list.length} via UNIVERSE_FILE)`);
    return { src: p, list };
  }
  // 2) prefer larger between universe_all.txt and universe.txt
  const pAll = path.resolve(baseDir, "universe_all.txt");
  const pDef = path.resolve(baseDir, "universe.txt");
  const listAll = readLines(pAll);
  const listDef = readLines(pDef);
  let src = pDef, list = listDef;
  if (listAll.length > listDef.length) {
    src = pAll; list = listAll;
  }
  console.log(`[universe] auto-picked ${src} (all:${listAll.length}, def:${listDef.length})`);
  return { src, list };
}

let __cache = null;
function getUniverseCached(qTickers, forceFile = false) {
  if (!forceFile && qTickers && String(qTickers).trim()) {
    const list = String(qTickers).split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
    return { mode: "query", src: "query", list };
  }
  if (!__cache) __cache = loadUniverseFromFile();
  return { mode: "file", src: __cache.src, list: __cache.list };
}
function invalidateUniverseCache() { __cache = null; }

module.exports = {
  loadUniverseFromFile,
  getUniverseCached,
  invalidateUniverseCache
};