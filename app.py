import os
import io
import typing as t
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil import parser as dtp
import streamlit as st
from dateutil import tz as dttz
from datetime import time as _dtime

# --- Secrets/env helper ---

def _get_secret(name: str, default: str = "") -> str:
    try:
        # prefer Streamlit Secrets if available (Cloud/local .streamlit/secrets.toml)
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets.get(name, default))
    except Exception:
        pass
    return os.getenv(name, default)

# --- Build diagnostics (helps verify what file/revision Streamlit is running) ---

def _git_info() -> t.Optional[dict]:
    """Return {'branch','commit','date','repo'} if this is a git checkout, else None."""
    try:
        import subprocess, pathlib
        repo_dir = pathlib.Path(__file__).resolve().parent
        # walk up to find a .git directory
        root = None
        for p in [repo_dir] + list(repo_dir.parents):
            if (p / ".git").exists():
                root = p
                break
        if root is None:
            return None
        def _run(cmd: list[str]) -> str:
            return subprocess.check_output(cmd, cwd=str(root)).decode().strip()
        branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"]) or "(detached)"
        commit = _run(["git", "rev-parse", "--short", "HEAD"]) or "?"
        date   = _run(["git", "show", "-s", "--format=%ci", "HEAD"]) or "?"
        return {"branch": branch, "commit": commit, "date": date, "repo": str(root)}
    except Exception:
        return None

# ==========================
# Data models & providers
# ==========================

class OptionQuote(t.TypedDict):
    provider: str
    option_symbol: str
    underlying: str
    type: str  # 'put'/'call'
    strike: float
    expiration: str  # ISO date
    bid: float
    ask: float
    last: t.Optional[float]
    volume: t.Optional[int]
    open_interest: t.Optional[int]
    underlying_price: t.Optional[float]
    exch: t.Optional[str]
    updated: t.Optional[str]

class Provider:
    name: str
    def get_put_quotes(self, symbol: str, min_dte: int, max_dte: int) -> list[OptionQuote]:
        raise NotImplementedError

# ---- Tradier ----

class TradierProvider(Provider):
    name = "Tradier"
    def __init__(self, token: str, endpoint: str = "https://api.tradier.com"):
        self.token = token
        self.endpoint = endpoint.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        })

    def _expirations(self, symbol: str) -> list[str]:
        url = f"{self.endpoint}/v1/markets/options/expirations"
        r = self.session.get(url, params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"}, timeout=30)
        r.raise_for_status()
        data = r.json()
        exps = data.get("expirations", {}).get("date", [])
        if isinstance(exps, str):
            exps = [exps]
        return exps

    def _chain(self, symbol: str, expiration: str) -> list[dict]:
        url = f"{self.endpoint}/v1/markets/options/chains"
        r = self.session.get(url, params={"symbol": symbol, "expiration": expiration, "greeks": "false"}, timeout=60)
        r.raise_for_status()
        data = r.json()
        options = data.get("options", {}).get("option", [])
        if isinstance(options, dict):
            options = [options]
        return options

    def get_put_quotes(self, symbol: str, min_dte: int, max_dte: int) -> list[OptionQuote]:
        out: list[OptionQuote] = []
        today = datetime.now(timezone.utc).date()
        try:
            expirations = self._expirations(symbol)
        except Exception:
            return out
        for exp in expirations:
            try:
                d = dtp.parse(exp).date()
            except Exception:
                continue
            dte = (d - today).days
            if dte < min_dte or dte > max_dte:
                continue
            try:
                chain = self._chain(symbol, exp)
            except Exception:
                continue
            for c in chain:
                if str(c.get("option_type", "")).lower() != "put":
                    continue
                bid = float(c.get("bid", 0) or 0)
                ask = float(c.get("ask", 0) or 0)
                strike = float(c.get("strike", 0) or 0)
                if strike <= 0:
                    continue
                out.append(OptionQuote(
                    provider=self.name,
                    option_symbol=str(c.get("symbol", "")),
                    underlying=symbol,
                    type="put",
                    strike=strike,
                    expiration=str(d),
                    bid=bid,
                    ask=ask,
                    last=float(c.get("last", 0) or 0),
                    volume=int(c.get("volume", 0) or 0),
                    open_interest=int(c.get("open_interest", 0) or 0),
                    underlying_price=float(c.get("underlying_price", 0) or 0),
                    exch=str(c.get("root_symbol", "")),
                    updated=None,
                ))
        return out

# ---- Polygon ----

class PolygonProvider(Provider):
    name = "Polygon"
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base = "https://api.polygon.io"
        self.sess = requests.Session()
        self._qcache: dict[str, tuple[float, float, float]] = {}

    def _trade_latest(self, option_symbol: str) -> float:
        """Return latest trade price for an option symbol (0.0 if unavailable)."""
        try:
            r = self.sess.get(
                f"{self.base}/v3/trades/{option_symbol}/latest",
                params={"apiKey": self.api_key},
                timeout=20,
            )
            if r.status_code == 200:
                px = (r.json() or {}).get("results", {}).get("price")
                return float(px or 0)
        except Exception:
            pass
        return 0.0

    def _prev_close(self, option_symbol: str) -> float:
        """Return previous close price for an option symbol (0.0 if unavailable)."""
        try:
            r = self.sess.get(
                f"{self.base}/v2/aggs/ticker/{option_symbol}/prev",
                params={"adjusted": "true", "apiKey": self.api_key},
                timeout=20,
            )
            if r.status_code == 200:
                results = (r.json() or {}).get("results") or []
                if results:
                    return float(results[0].get("c") or 0)  # previous close
        except Exception:
            pass
        return 0.0

    def _is_after_hours_et(self) -> bool:
        """Return True if now is outside ~9:30–16:05 ET trading session."""
        try:
            now_utc = datetime.now(timezone.utc)
            et = dttz.gettz("America/New_York")
            now_et = now_utc.astimezone(et)
            t = now_et.time()
            return (t >= _dtime(16, 5)) or (t < _dtime(9, 30))
        except Exception:
            return True

    def _snapshot_quote(self, option_symbol: str) -> tuple[float, float, float]:
        """Best-effort snapshot fallback: (bid, ask, last). Caches by option symbol."""
        try:
            if option_symbol in self._qcache:
                return self._qcache[option_symbol]
            r = self.sess.get(
                f"{self.base}/v3/snapshot/options/{option_symbol}",
                params={"apiKey": self.api_key},
                timeout=20,
            )
            if r.status_code == 200:
                js = (r.json() or {}).get("results", {}) or {}
                lq = js.get("last_quote") or {}
                bid = float(lq.get("bid", 0) or 0)
                ask = float(lq.get("ask", 0) or 0)
                lt = js.get("last_trade") or {}
                last = float(lt.get("price", 0) or 0)
                if last <= 0:
                    last = float((js.get("day") or {}).get("close", 0) or 0)
                tup = (bid, ask, last)
                self._qcache[option_symbol] = tup
                return tup
        except Exception:
            pass
        return 0.0, 0.0, 0.0

    def _iter_contracts(self, symbol: str) -> t.Iterator[dict]:
        """
        Yield reference option contracts for an underlying across *all* pages.
        This avoids the default 1000-result cap that was hiding far-dated expirations.
        """
        params = {
            "underlying_ticker": symbol,
            "contract_type": "put",
            "limit": 1000,
            "apiKey": self.api_key,
        }
        url = f"{self.base}/v3/reference/options/contracts"
        while True:
            r = self.sess.get(url, params=params, timeout=60)
            r.raise_for_status()
            js = r.json() or {}
            results = js.get("results") or []
            for row in results:
                yield row
            # polygon v3 returns either 'next_url' or a cursor token
            next_url = js.get("next_url")
            next_cursor = js.get("next_cursor") or js.get("cursor")
            if next_url:
                # When next_url is absolute, call it directly; otherwise pass cursor
                url = next_url
                params = {}  # next_url already has query string incl. apiKey
            elif next_cursor:
                url = f"{self.base}/v3/reference/options/contracts"
                params = {"cursor": next_cursor, "apiKey": self.api_key}
            else:
                break

    def _nbbo(self, option_symbol: str) -> dict:
        r = self.sess.get(f"{self.base}/v3/quotes/{option_symbol}/nbbo/latest", params={"apiKey": self.api_key}, timeout=30)
        if r.status_code != 200:
            return {}
        return r.json().get("results", {})

    def get_put_quotes(self, symbol: str, min_dte: int, max_dte: int) -> list[OptionQuote]:
        """
        Paginate through Polygon reference contracts so we include *all* expirations.
        For each contract within the requested DTE window, fetch NBBO and
        apply robust after-hours fallbacks to populate bid/ask/last.
        """
        out: list[OptionQuote] = []
        today = datetime.now(timezone.utc).date()

        try:
            for c in self._iter_contracts(symbol):
                # Basic contract fields
                opt = c.get("ticker") or c.get("options_ticker")
                if not opt:
                    continue
                try:
                    exp_txt = c.get("expiration_date") or c.get("expiration")
                    exp_date = dtp.parse(str(exp_txt)).date()
                except Exception:
                    continue

                # DTE filter
                dte = (exp_date - today).days
                if dte < min_dte or dte > max_dte:
                    continue

                strike = float(c.get("strike_price", 0) or 0)
                if strike <= 0:
                    continue

                # Primary quote via NBBO
                nbbo = self._nbbo(opt) or {}
                bid = float(nbbo.get("bid_price", 0) or 0)
                ask = float(nbbo.get("ask_price", 0) or 0)

                # Fallbacks (snapshot → last trade → previous close)
                last_px = 0.0
                if bid <= 0 and ask <= 0:
                    s_bid, s_ask, s_last = self._snapshot_quote(opt)
                    if s_bid > 0 or s_ask > 0:
                        bid, ask = s_bid, s_ask
                        last_px = s_last or 0.0
                    else:
                        last_px = self._trade_latest(opt)
                        if last_px <= 0:
                            last_px = self._prev_close(opt)
                    # When after-hours, synthesize a mark from last price if needed
                    if (bid <= 0 and ask <= 0) and last_px > 0 and self._is_after_hours_et():
                        bid = last_px
                        ask = last_px

                out.append(OptionQuote(
                    provider=self.name,
                    option_symbol=opt,
                    underlying=symbol,
                    type="put",
                    strike=strike,
                    expiration=str(exp_date),
                    bid=bid,
                    ask=ask,
                    last=last_px if last_px > 0 else None,
                    volume=None,
                    open_interest=None,
                    underlying_price=None,
                    exch=None,
                    updated=nbbo.get("sip_timestamp"),
                ))
        except Exception:
            # Fail soft and return whatever we accumulated
            pass

        return out

# ==========================
# UI
# ==========================


st.set_page_config(page_title="Inflated Put Tracker (Bid/Strike% Scanner)", page_icon="📈", layout="wide")
st.title("📈 Inflated Put Tracker — Bid/Strike% Scanner")
_build_ts = datetime.fromtimestamp(os.path.getmtime(__file__)).strftime("%Y-%m-%d %H:%M:%S")
st.caption(f"Build: {os.path.basename(__file__)} @ {_build_ts}")
# Display path & git info (useful to catch Cloud running an older branch/file)
st.caption(f"File: {os.path.abspath(__file__)}")
_git = _git_info()
if _git:
    st.caption(f"Git: {_git['branch']} @ {_git['commit']} ({_git['date']}) — repo {_git['repo']}")
else:
    st.caption("Git: not a git checkout or git unavailable in this environment")

with st.sidebar:
    st.header("Data Source")
    _poly_key_default = _get_secret("POLYGON_KEY", "")
    _default_provider_index = 1 if _poly_key_default else 0
    provider_choice = st.selectbox("Provider", ["Tradier", "Polygon", "CSV only"], index=_default_provider_index)
    cred = ""
    if provider_choice == "Tradier":
        cred = st.text_input("Tradier Token", type="password", value=_get_secret("TRADIER_TOKEN", ""))
    elif provider_choice == "Polygon":
        # Prefill Polygon key from secrets/env if present
        cred = st.text_input("Polygon API Key", type="password", value=_poly_key_default)

    st.header("Symbols")

    def _parse_symbols(text: str) -> list[str]:
        raw = text.replace("\n", ",").replace("\t", ",").replace(" ", ",")
        return sorted({s.strip().upper() for s in raw.split(",") if s.strip()})

    # Default to using the repo universe file (no prefill)
    symbol_source = st.radio("Choose symbols from:", ["Custom input", "Repo list file"], index=1, horizontal=True)

    repo_dir = os.path.dirname(__file__)
    fallback_dir = os.path.expanduser("~/Documents/inflated-puts-tracker")
    candidate_files = [
        os.path.join(repo_dir, "universe_all.txt"),
        os.path.join(repo_dir, "universe.txt"),
        os.path.join(repo_dir, "symbols.txt"),
        os.path.join(repo_dir, "nasdaqlisted.txt"),
        os.path.join(repo_dir, "otherlisted.txt"),
        os.path.join(fallback_dir, "universe_all.txt"),
        os.path.join(fallback_dir, "universe.txt"),
        os.path.join(fallback_dir, "nasdaqlisted.txt"),
        os.path.join(fallback_dir, "otherlisted.txt"),
    ]
    existing_files = [p for p in candidate_files if os.path.exists(p)]

    symbols: list[str] = []
    if symbol_source == "Custom input":
        syms_text = st.text_area(
            "Symbols (comma/space/newline separated)",
            value="",  # no prefill
            height=90,
        )
        symbols = _parse_symbols(syms_text)
    else:
        if not existing_files:
            st.error("No universe file found in the repo. Expected `universe_all.txt`, `universe.txt`, `nasdaqlisted.txt`, or `otherlisted.txt`.")
            symbols = []
        else:
            chosen_file = st.selectbox("Universe file", existing_files, format_func=lambda p: os.path.basename(p))
            try:
                with open(chosen_file, "r", encoding="utf-8", errors="ignore") as fh:
                    content = fh.read()
                symbols = _parse_symbols(content)
            except Exception as e:
                st.error(f"Failed to read universe file: {e}")
                symbols = []
        symbol_limit = st.number_input("(Optional) Limit number of symbols to scan", min_value=50, step=50, value=min(7000, max(50, len(symbols) or 50)))
        if symbols:
            symbols = symbols[: int(symbol_limit)]

    st.header("Filters")
    target_pct = st.number_input("Target Bid/Strike %", min_value=0.0, step=0.5, value=10.0)
    min_dte = st.number_input("Min DTE (days)", min_value=0, step=1, value=7)
    max_dte = st.number_input("Max DTE (days)", min_value=1, step=1, value=730)
    min_bid = st.number_input("Min Bid ($)", min_value=0.0, step=0.05, value=0.10)
    min_oi = st.number_input("Min Open Interest", min_value=0, step=10, value=50)
    min_vol = st.number_input("Min Volume (today)", min_value=0, step=10, value=0)
    moneyness = st.selectbox("Moneyness (requires underlying price in feed)", ["Any", "OTM only", "ITM only"], index=0)

    use_mark_fallback = st.checkbox("Use mid price when bid = 0 (fallback)", value=True)
    st.session_state["use_mark_fallback"] = use_mark_fallback

    max_rows = st.slider("Max rows", min_value=100, max_value=5000, value=1000, step=100)

    uploaded_quotes = st.file_uploader("Or upload an option quotes CSV to filter", type=["csv"])  # optional
    run = st.button("Run scan 🚀")

st.caption("Start with a small list, confirm behavior, then widen to big universes/watchlists.")

# === Workspace Tabs ===
scan_tab, earn_ticker_tab, earn_cal_tab, earn_list_tab = st.tabs([
    "Options Scanner",
    "Earnings — Single Ticker",
    "Earnings — Calendar",
    "Earnings — CSV/Paste",
])

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Robust numeric parsing
    df["bid"]   = pd.to_numeric(df.get("bid"), errors="coerce").fillna(0.0)
    df["ask"]   = pd.to_numeric(df.get("ask"), errors="coerce").fillna(0.0)
    df["last"]  = pd.to_numeric(df.get("last"), errors="coerce").fillna(0.0)
    df["strike"] = pd.to_numeric(df.get("strike"), errors="coerce").fillna(0.0)

    # Effective bid: prefer bid; if 0, optionally use mid (bid+ask)/2; then fallback to last
    try:
        _use_mark = st.session_state.get("use_mark_fallback", True)
    except Exception:
        _use_mark = True
    eff = df["bid"].copy()
    # mid price when bid is 0 and ask>0
    mid = (df["bid"].fillna(0) + df["ask"].fillna(0)) / 2.0
    if _use_mark:
        eff = eff.where(eff > 0, mid)
    # fallback to last when still 0
    eff = eff.where(eff > 0, df["last"])  # last may be 0 if unavailable
    df["eff_bid"] = pd.to_numeric(eff, errors="coerce").fillna(0.0)

    # Avoid divide-by-zero
    denom = df["strike"].replace(0, pd.NA)
    df["bid_strike_pct"] = (df["eff_bid"].astype(float) / denom).astype(float) * 100.0
    df["bid_strike_pct"] = df["bid_strike_pct"].fillna(0.0)

    # Parse expiration to datetime once; tolerate bad values
    exp = pd.to_datetime(df.get("expiration"), errors="coerce")
    # Compute DTE defensively
    today_norm = pd.Timestamp.today().normalize()
    dte_series = (exp.dt.normalize() - today_norm).dt.days
    df["dte"] = pd.to_numeric(dte_series, errors="coerce")
    # Format expiration for display
    df["expiration"] = exp.dt.date.astype("string")
    return df

def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = compute_metrics(df)
    mask = (
        (df["bid_strike_pct"] >= float(target_pct)) &
        (df["dte"].between(int(min_dte), int(max_dte))) &
        (df["eff_bid"] >= float(min_bid))
    )
    if "open_interest" in df.columns:
        mask &= (pd.to_numeric(df["open_interest"], errors="coerce").fillna(0) >= int(min_oi))
    if "volume" in df.columns:
        mask &= (pd.to_numeric(df["volume"], errors="coerce").fillna(0) >= int(min_vol))
    # optional moneyness if underlying_price available
    if moneyness != "Any" and "underlying_price" in df.columns and df["underlying_price"].notna().any():
        up = pd.to_numeric(df["underlying_price"], errors="coerce")
        if moneyness == "OTM only":
            mask &= (pd.to_numeric(df["strike"], errors="coerce") < up)
        elif moneyness == "ITM only":
            mask &= (pd.to_numeric(df["strike"], errors="coerce") >= up)
    out = df[mask].sort_values(["bid_strike_pct", "eff_bid"], ascending=[False, False])
    return out


def has_options(provider_choice: str, cred: str, symbol: str) -> bool:
    try:
        if provider_choice == "Tradier" and cred:
            prov = TradierProvider(cred)
            exps = prov._expirations(symbol)
            return bool(exps)
        if provider_choice == "Polygon" and cred:
            prov = PolygonProvider(cred)
            exps = prov._expirations(symbol)
            return bool(exps)
    except Exception:
        return False
    return False

# --- Optionability marker helper ---
def optionability_mark(provider_choice: str, cred: str, symbol: str) -> tuple[str, str]:
    """
    Return (status, display_symbol).
    status: 'yes' (confirmed optionable), 'unknown' (couldn't verify), or 'no' (confirmed not optionable).
    display_symbol: symbol with '*' for yes, '?' for unknown, plain if 'no'.
    """
    try:
        if provider_choice in ("Tradier", "Polygon") and cred:
            if has_options(provider_choice, cred, symbol):
                return "yes", f"{symbol}*"
            else:
                return "no", symbol
        # No provider/key to verify => unknown
        return "unknown", f"{symbol}?"
    except Exception:
        return "unknown", f"{symbol}?"

# --- Earnings helpers (symbol + calendar) ---
YF_HEADERS = {"User-Agent": "Mozilla/5.0"}
NQ_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# --- Yahoo Finance HTML calendar headers ---
YF_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

# Benzinga & EarningsWhispers HTML headers
BENZ_HEADERS = {
    "User-Agent": YF_PAGE_HEADERS["User-Agent"],
    "Accept": YF_PAGE_HEADERS["Accept"],
    "Accept-Language": YF_PAGE_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
}
EW_HEADERS = {
    "User-Agent": YF_PAGE_HEADERS["User-Agent"],
    "Accept": YF_PAGE_HEADERS["Accept"],
    "Accept-Language": YF_PAGE_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
}

# Benzinga News headers (same UA)
BENZ_NEWS_HEADERS = {
    "User-Agent": YF_PAGE_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": YF_PAGE_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
}

# --- MarketBeat and EarningsWhispers single-ticker fallback headers and scrapers ---
MB_HEADERS = {
    "User-Agent": YF_PAGE_HEADERS["User-Agent"],
    "Accept": YF_PAGE_HEADERS["Accept"],
    "Accept-Language": YF_PAGE_HEADERS["Accept-Language"],
    "Cache-Control": "no-cache",
}

def _earnings_from_marketbeat(symbol: str) -> list[dict]:
    """Scrape MarketBeat single-ticker earnings page for a date (best-effort)."""
    import re
    sym = symbol.strip().upper()
    out: list[dict] = []
    bases = [
        f"https://www.marketbeat.com/stocks/NASDAQ/{sym}/earnings/",
        f"https://www.marketbeat.com/stocks/NYSE/{sym}/earnings/",
    ]
    pat_iso = re.compile(r"(20\d{2}-\d{2}-\d{2})")
    pat_long = re.compile(r"([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})")
    for url in bases:
        try:
            r = requests.get(url, headers=MB_HEADERS, timeout=30)
            if r.status_code != 200 or not r.text:
                continue
            html = r.text
            m = pat_iso.search(html) or pat_long.search(html)
            if m:
                dt_txt = m.group(1)
                try:
                    dt = pd.to_datetime(dt_txt, errors="coerce").date()
                    if pd.notnull(dt):
                        out.append({"source": "MarketBeat", "date": dt})
                        break
                except Exception:
                    continue
        except Exception:
            continue
    return out


def _earnings_from_earningswhispers_single(symbol: str) -> list[dict]:
    """Best‑effort scrape of EarningsWhispers single-ticker page for a date."""
    import re
    sym = symbol.strip().lower()
    url = f"https://www.earningswhispers.com/stocks/{sym}"
    out: list[dict] = []
    try:
        r = requests.get(url, headers=EW_HEADERS, timeout=30)
        if r.status_code != 200 or not r.text:
            return out
        html = r.text
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", html) or re.search(r"([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})", html)
        if m:
            dt_txt = m.group(1)
            dt = pd.to_datetime(dt_txt, errors="coerce").date()
            if pd.notnull(dt):
                out.append({"source": "EarningsWhispers", "date": dt})
    except Exception:
        pass
    return out

def _earnings_from_yahoo(symbol: str) -> list[dict]:
    """Return list of {source,date} from Yahoo quoteSummary calendarEvents/earnings."""
    out = []
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        # Request both modules; some tickers only populate one
        params = {"modules": "calendarEvents,earnings"}
        r = requests.get(url, params=params, headers=YF_HEADERS, timeout=20)
        r.raise_for_status()
        js = r.json() or {}
        res = (js.get("quoteSummary", {}) or {}).get("result", []) or []
        if not res:
            return out
        node = res[0] or {}
        cal = node.get("calendarEvents") or {}
        earn = cal.get("earnings") or {}
        dates = earn.get("earningsDate") or []
        for d in dates:
            dt = None
            if isinstance(d, dict):
                dt = d.get("fmt") or d.get("raw")
            else:
                dt = d
            if isinstance(dt, (int, float)):
                dt = pd.to_datetime(dt, unit="s", utc=True).date().isoformat()
            if isinstance(dt, str):
                out.append({"source": "Yahoo", "date": pd.to_datetime(dt, errors="coerce").date()})
        # Fallback: sometimes currentQuarterDate appears only under earningsChart
        enode = node.get("earnings") or {}
        qd = (enode.get("earningsChart") or {}).get("currentQuarterDate")
        if qd and not any(x.get("date") for x in out):
            try:
                out.append({"source": "Yahoo", "date": pd.to_datetime(qd).date()})
            except Exception:
                pass
    except Exception:
        pass
    return out

# --- Yahoo v7 quote fallback (earningsTimestamp window) ---

def _earnings_from_yahoo_quote(symbol: str) -> list[dict]:
    out: list[dict] = []
    try:
        base = "https://query1.finance.yahoo.com/v7/finance/quote"
        r = requests.get(base, params={"symbols": symbol}, headers=YF_HEADERS, timeout=20)
        if r.status_code != 200:
            return out
        res = (r.json() or {}).get("quoteResponse", {}).get("result", [])
        if not res:
            return out
        row = res[0]
        for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd"):
            val = row.get(key)
            if not val:
                continue
            try:
                d = pd.to_datetime(int(val), unit="s", utc=True).date()
                out.append({"source": "YahooQuote", "date": d})
            except Exception:
                continue
    except Exception:
        pass
    return out

def _earnings_from_polygon(symbol: str, api_key: str | None) -> list[dict]:
    """Best‑effort pull from Polygon if key provided. Returns list[{source,date}]."""
    out = []
    if not api_key:
        return out
    try:
        # Try vX endpoint; ignore if not available
        url = "https://api.polygon.io/vX/reference/earnings"
        r = requests.get(url, params={"ticker": symbol, "limit": 5, "apiKey": api_key}, timeout=20)
        if r.status_code == 200:
            js = r.json()
            for row in js.get("results", []) or []:
                dt = row.get("fiscal_period_end_date") or row.get("report_date") or row.get("announcement_date")
                if dt:
                    out.append({"source": "Polygon", "date": pd.to_datetime(dt, errors="coerce").date()})
    except Exception:
        pass
    return out

def get_symbol_earnings_multi(symbol: str, polygon_key: str | None = None) -> pd.DataFrame:
    """Aggregate symbol earnings dates from multiple sources.
    1) Direct symbol lookups (Yahoo/Polygon)
    2) Fallback: pull merged calendar for a loose window (today-30d .. today+60d) and filter by symbol
    """
    symbol = symbol.strip().upper()
    rows: list[dict] = []
    # 1) direct
    rows += _earnings_from_yahoo(symbol)
    rows += _earnings_from_yahoo_quote(symbol)
    rows += _earnings_from_polygon(symbol, polygon_key)
    # Additional public web fallbacks
    rows += _earnings_from_marketbeat(symbol)
    rows += _earnings_from_earningswhispers_single(symbol)
    # 2) range fallback
    try:
        start = (pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=30)).date()
        end = (pd.Timestamp.utcnow().normalize() + pd.Timedelta(days=60)).date()
        df_range, _stats = fetch_calendar_range_multi(pd.to_datetime(start), pd.to_datetime(end))
        if not df_range.empty:
            df_match = df_range[df_range["symbol"].astype(str).str.upper() == symbol]
            for _, r in df_match.iterrows():
                rows.append({"source": str(r.get("source","Calendar")), "date": pd.to_datetime(r.get("date"), errors="coerce").date()})
    except Exception:
        pass
    # de-dupe and return
    if not rows:
        return pd.DataFrame(columns=["symbol","date","source"])
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["date"]).copy()
    df["symbol"] = symbol
    df = df.drop_duplicates(["symbol","date","source"]).sort_values(["date","source"]).reset_index(drop=True)
    return df[["symbol","date","source"]]

def fetch_nasdaq_calendar_for_date(d: pd.Timestamp) -> pd.DataFrame:
    """Fetch Nasdaq earnings for a single date; returns DataFrame with symbol, date, session."""
    try:
        url = "https://api.nasdaq.com/api/calendar/earnings"
        r = requests.get(url, params={"date": d.strftime("%Y-%m-%d")}, headers=NQ_HEADERS, timeout=20)
        r.raise_for_status()
        js = r.json() or {}
        data = (((js.get("data") or {}).get("calendar") or {}).get("rows") or [])
        rows = []
        for it in data:
            sym = (it.get("symbol") or "").strip().upper()
            sess = (it.get("time") or "").strip().upper()  # "BMO"/"AMC"/"TAS"
            if not sym:
                continue
            if sess not in {"BMO","AMC","TAS"}:
                sess = ""  # unknown
            rows.append({"symbol": sym, "date": d.date(), "session": sess, "source": "Nasdaq"})
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame(columns=["symbol","date","session","source"])

# --- Benzinga earnings calendar (per-day HTML scrape) ---

def fetch_benzinga_calendar_for_date(d: pd.Timestamp) -> pd.DataFrame:
    """Scrape Benzinga earnings calendar for a given date. Returns symbol/date/session/source."""
    try:
        url = "https://www.benzinga.com/calendars/earnings"
        resp = requests.get(url, params={"date": d.strftime("%Y-%m-%d")}, headers=BENZ_HEADERS, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        if not tables:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        df_any = None
        for tdf in tables:
            cols = [str(c).strip().lower() for c in tdf.columns]
            if any(c.startswith("symbol") or c == "ticker" for c in cols):
                df_any = tdf
                break
        if df_any is None or df_any.empty:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        sym_col = next((c for c in df_any.columns if str(c).strip().lower().startswith("symbol") or str(c).strip().lower()=="ticker"), None)
        time_col = next((c for c in df_any.columns if str(c).strip().lower() in ("time","report time","announcement time","when")), None)
        out_rows = []
        for _, row in df_any.iterrows():
            sym = str(row.get(sym_col, "")).strip().upper() if sym_col else ""
            if not sym or sym == "NAN":
                continue
            sess = _map_yahoo_time_to_session(str(row.get(time_col, "")) if time_col else "")
            out_rows.append({"symbol": sym, "date": d.date(), "session": sess, "source": "Benzinga"})
        return pd.DataFrame(out_rows)
    except Exception:
        return pd.DataFrame(columns=["symbol","date","session","source"])

# --- EarningsWhispers earnings calendar (per-day HTML scrape) ---

def fetch_earningswhispers_calendar_for_date(d: pd.Timestamp) -> pd.DataFrame:
    """Scrape EarningsWhispers calendar for a given date. Best-effort as layout can change."""
    try:
        url = "https://www.earningswhispers.com/calendar"
        resp = requests.get(url, params={"sb": "p", "d": d.strftime("%Y-%m-%d")}, headers=EW_HEADERS, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        if not tables:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        df_any = None
        for tdf in tables:
            cols = [str(c).strip().lower() for c in tdf.columns]
            if any("symbol" in c or c == "ticker" for c in cols):
                df_any = tdf
                break
        if df_any is None or df_any.empty:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        sym_col = next((c for c in df_any.columns if "symbol" in str(c).strip().lower() or str(c).strip().lower()=="ticker"), None)
        time_col = next((c for c in df_any.columns if str(c).strip().lower() in ("time","when","announcement time","report time")), None)
        out_rows = []
        for _, row in df_any.iterrows():
            sym = str(row.get(sym_col, "")).strip().upper() if sym_col else ""
            if not sym or sym == "NAN":
                continue
            sess = _map_yahoo_time_to_session(str(row.get(time_col, "")) if time_col else "")
            out_rows.append({"symbol": sym, "date": d.date(), "session": sess, "source": "EarningsWhispers"})
        return pd.DataFrame(out_rows)
    except Exception:
        return pd.DataFrame(columns=["symbol","date","session","source"])

# --- Yahoo Finance earnings calendar HTML fetcher ---
def _map_yahoo_time_to_session(val: str) -> str:
    v = (val or "").strip().lower()
    if not v:
        return ""
    if "before" in v or "pre-market" in v or "bmo" in v:
        return "BMO"
    if "after" in v or "post-market" in v or "amc" in v:
        return "AMC"
    return ""  # unknown / time tbd


def fetch_yahoo_calendar_for_date(d: pd.Timestamp) -> pd.DataFrame:
    """Scrape Yahoo Finance earnings calendar table for a given day.
    Returns DataFrame with columns: symbol, date, session, source.
    """
    try:
        url = "https://finance.yahoo.com/calendar/earnings"
        params = {"day": d.strftime("%Y-%m-%d")}
        resp = requests.get(url, params=params, headers=YF_PAGE_HEADERS, timeout=30)
        resp.raise_for_status()
        # Use pandas to parse tables; pick the one with a 'Symbol' column
        tables = pd.read_html(resp.text)
        if not tables:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        df_any = None
        for tdf in tables:
            cols = [str(c).strip().lower() for c in tdf.columns]
            if any(c.startswith("symbol") for c in cols):
                df_any = tdf
                break
        if df_any is None or df_any.empty:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        # Normalize expected columns
        # Yahoo often uses 'Symbol' and 'Time'
        col_map = {c: str(c).strip().lower() for c in df_any.columns}
        # best-effort column access
        sym_col = [c for c in df_any.columns if str(c).strip().lower().startswith("symbol")]
        time_col = [c for c in df_any.columns if str(c).strip().lower() in ("time", "earnings call time", "event time")]
        if not sym_col:
            return pd.DataFrame(columns=["symbol","date","session","source"])
        sym_col = sym_col[0]
        time_col = time_col[0] if time_col else None
        out_rows = []
        for _, row in df_any.iterrows():
            sym = str(row.get(sym_col, "")).strip().upper()
            if not sym or sym == "nan":
                continue
            sess = _map_yahoo_time_to_session(str(row.get(time_col, "")) if time_col else "")
            out_rows.append({"symbol": sym, "date": d.date(), "session": sess, "source": "Yahoo"})
        return pd.DataFrame(out_rows)
    except Exception:
        return pd.DataFrame(columns=["symbol","date","session","source"])


def fetch_nasdaq_calendar_range(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    days = pd.date_range(start, end, freq="D")
    all_df = []
    prog = st.progress(0, text="Fetching earnings calendar…")
    for i, d in enumerate(days, 1):
        prog.progress(int(i/len(days)*100), text=f"{d.date()}…")
        all_df.append(fetch_nasdaq_calendar_for_date(d))
    prog.empty()
    if not all_df:
        return pd.DataFrame(columns=["symbol","date","session","source"])
    df = pd.concat(all_df, ignore_index=True)
    if not df.empty:
        df = df.drop_duplicates(["symbol","date","session"]).sort_values(["date","symbol"]).reset_index(drop=True)
    return df

# --- Multi-source calendar aggregator (Nasdaq + Yahoo + Benzinga + EarningsWhispers) ---
def fetch_calendar_range_multi(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.DataFrame, dict]:
    days = pd.date_range(start, end, freq="D")
    merged: list[pd.DataFrame] = []
    stats = {"nasdaq": 0, "yahoo": 0, "benzinga": 0, "earningswhispers": 0}
    prog = st.progress(0, text="Fetching earnings calendar…")
    for i, d in enumerate(days, 1):
        prog.progress(int(i/len(days)*100), text=f"{d.date()}…")
        df_nq = fetch_nasdaq_calendar_for_date(d)
        df_yf = fetch_yahoo_calendar_for_date(d)
        df_bz = fetch_benzinga_calendar_for_date(d)
        df_ew = fetch_earningswhispers_calendar_for_date(d)
        if not df_nq.empty:
            stats["nasdaq"] += len(df_nq); merged.append(df_nq)
        if not df_yf.empty:
            stats["yahoo"] += len(df_yf); merged.append(df_yf)
        if not df_bz.empty:
            stats["benzinga"] += len(df_bz); merged.append(df_bz)
        if not df_ew.empty:
            stats["earningswhispers"] += len(df_ew); merged.append(df_ew)
    prog.empty()
    if not merged:
        return pd.DataFrame(columns=["symbol","date","session","source"]), stats
    df = pd.concat(merged, ignore_index=True)
    if not df.empty:
        df = df.drop_duplicates(["symbol","date","session"]).sort_values(["date","symbol"]).reset_index(drop=True)
    return df, stats

# === Implied Volatility helpers ===

def _tradier_atm_iv(token: str, symbol: str) -> t.Optional[float]:
    """Estimate underlying IV via Tradier by sampling near‑ATM options around ~30 DTE.
    Returns IV as a decimal (e.g., 0.42) or None.
    """
    try:
        sess = requests.Session()
        sess.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/json"})
        # expirations
        r = sess.get("https://api.tradier.com/v1/markets/options/expirations",
                     params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"}, timeout=20)
        r.raise_for_status()
        exps = r.json().get("expirations", {}).get("date", [])
        if isinstance(exps, str):
            exps = [exps]
        if not exps:
            return None
        today = datetime.now(timezone.utc).date()
        # pick expiration nearest 30 DTE
        def _dte(d):
            try:
                return abs((dtp.parse(d).date() - today).days - 30)
            except Exception:
                return 10**9
        exp = sorted(exps, key=_dte)[0]
        # fetch chain with greeks
        r = sess.get("https://api.tradier.com/v1/markets/options/chains",
                     params={"symbol": symbol, "expiration": exp, "greeks": "true"}, timeout=30)
        r.raise_for_status()
        opts = r.json().get("options", {}).get("option", [])
        if isinstance(opts, dict):
            opts = [opts]
        if not opts:
            return None
        # pick near‑ATM within 2% moneyness
        rows = []
        for o in opts:
            up = float(o.get("underlying_price", 0) or 0)
            k  = float(o.get("strike", 0) or 0)
            g  = o.get("greeks") or {}
            iv = g.get("smv_vol")
            if not up or not k or iv in (None, "", 0) or up <= 0:
                continue
            if abs(k - up) / up <= 0.02:
                try:
                    rows.append(float(iv))
                except Exception:
                    pass
        if not rows:
            return None
        # median IV
        return float(pd.Series(rows).median())
    except Exception:
        return None


def _yahoo_iv(symbol: str) -> t.Optional[float]:
    """Best‑effort pull of impliedVolatility from Yahoo quoteSummary."""
    try:
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        params = {"modules": "summaryDetail,defaultKeyStatistics"}
        r = requests.get(url, params=params, headers=YF_HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        js = r.json().get("quoteSummary", {}).get("result", [])
        if not js:
            return None
        node = js[0]
        for section in ("summaryDetail", "defaultKeyStatistics"):
            sec = node.get(section) or {}
            iv = (sec.get("impliedVolatility") or {}).get("raw") if isinstance(sec.get("impliedVolatility"), dict) else sec.get("impliedVolatility")
            if iv is not None:
                try:
                    return float(iv)
                except Exception:
                    pass
        return None
    except Exception:
        return None


def get_underlying_iv(provider_choice: str, cred: str, symbol: str) -> tuple[t.Optional[float], str]:
    """Return (iv_decimal, source_label). Tries Tradier first (needs token), then Yahoo."""
    if provider_choice == "Tradier" and cred:
        iv = _tradier_atm_iv(cred, symbol)
        if iv is not None:
            return iv, "Tradier (ATM ~30D)"
    iv = _yahoo_iv(symbol)
    if iv is not None:
        return iv, "Yahoo"
    return None, ""

def collect_live(symbols: list[str], provider_choice: str, cred: str, min_dte: int, max_dte: int) -> pd.DataFrame:
    if provider_choice == "Tradier":
        if not cred:
            st.error("Please enter a Tradier token in the sidebar.")
            return pd.DataFrame()
        provider: Provider = TradierProvider(cred)
    elif provider_choice == "Polygon":
        if not cred:
            st.error("Please enter a Polygon API key in the sidebar.")
            return pd.DataFrame()
        provider = PolygonProvider(cred)
    else:
        st.error("Unsupported provider selected.")
        return pd.DataFrame()

    rows: list[OptionQuote] = []
    errors = []
    total_contracts = 0
    progress = st.progress(0, text="Starting…")
    status = st.empty()
    for idx, sym in enumerate(symbols, 1):
        progress.progress(int(idx / max(1, len(symbols)) * 100), text=f"Symbol {idx} of {len(symbols)} — {sym}")
        try:
            quotes = provider.get_put_quotes(sym, int(min_dte), int(max_dte))
            rows.extend(quotes)
            total_contracts += len(quotes)
            status.caption(f"{sym}: fetched {len(quotes)} put contracts — running total: {total_contracts}")
        except Exception as e:
            errors.append(f"{sym}: {e}")
            continue

    if errors:
        with st.expander("Show fetch errors (rate limits / symbols with no data)"):
            for msg in errors:
                st.write(msg)

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    st.caption(f"Scanned {len(symbols)} symbols; collected {len(rows)} put contracts.")
    return df

with scan_tab:
    results = None
    if uploaded_quotes is not None:
        try:
            df = pd.read_csv(io.BytesIO(uploaded_quotes.getvalue()))
            df.columns = [c.strip().lower() for c in df.columns]
            if "type" in df.columns:
                df = df[df["type"].str.lower() == "put"]
            results = filter_rows(df)
        except Exception as e:
            st.error(f"CSV error: {e}")

    if run and results is None:
        if not symbols:
            st.warning("Add at least one symbol or upload a CSV.")
        elif provider_choice == "CSV only":
            st.warning("Choose Tradier/Polygon or upload a CSV to scan.")
        else:
            live_df = collect_live(symbols, provider_choice, cred, int(min_dte), int(max_dte))
            if live_df.empty:
                st.warning("No data returned. Check keys, rate limits, or widen symbols/DTE.")
            else:
                # Pre-filter diagnostics
                pre = compute_metrics(live_df)
                total = len(pre)
                dte_mask = pre["dte"].between(int(min_dte), int(max_dte))
                bid_mask = pre["eff_bid"] >= float(min_bid)
                pre_in_dte = int(dte_mask.sum())
                pre_bid_ok = int((dte_mask & bid_mask).sum())
                st.caption(
                    f"Diagnostics — rows: {total} | in DTE range: {pre_in_dte} | in DTE and bid≥min: {pre_bid_ok}. "
                    f"(Target Bid/Strike % filter applied later: ≥{float(target_pct):.2f}%)"
                )
                # --- Debug table: verify bid/ask/eff_bid/strike feeding correctly ---
                debug_cols = [c for c in [
                    "option_symbol","provider","underlying","expiration","strike","bid","ask","eff_bid",
                    "bid_strike_pct","dte","open_interest","volume"
                ] if c in pre.columns]
                st.dataframe(
                    pre.sort_values(["bid_strike_pct","eff_bid","bid"], ascending=[False, False, False])[debug_cols].head(25),
                    use_container_width=True,
                )
                try:
                    show_sample = bool(dbg_sample)
                except NameError:
                    show_sample = False
                if show_sample:
                    st.subheader("Sample before filters (top 20 by Bid/Strike %)")
                    cols = [c for c in [
                        "provider","option_symbol","underlying","strike","expiration","bid","ask",
                        "bid_strike_pct","dte","open_interest","volume"
                    ] if c in pre.columns]
                    st.dataframe(
                        pre.sort_values(["bid_strike_pct","bid"], ascending=[False, False])[cols].head(20),
                        use_container_width=True,
                    )
                # Now filter normally
                results = filter_rows(live_df)

    if results is not None:
        if results.empty:
            # Show quick stats to verify ingestion when nothing matches
            with st.expander("Show debug stats (pre-filter)"):
                try:
                    st.write({
                        "rows": int(len(pre)),
                        "eff_bid>0": int((pre.get("eff_bid", 0) > 0).sum()),
                        "bid>0": int((pre.get("bid", 0) > 0).sum()),
                        "ask>0": int((pre.get("ask", 0) > 0).sum()),
                        "median_eff_bid": float(pd.to_numeric(pre.get("eff_bid", 0), errors="coerce").median()),
                        "median_strike": float(pd.to_numeric(pre.get("strike", 0), errors="coerce").median()),
                        "max_bid_strike_pct": float(pd.to_numeric(pre.get("bid_strike_pct", 0), errors="coerce").max()),
                    })
                except Exception:
                    st.write("(no debug stats)")
            st.warning("No matches with current filters. Try lowering Target %, widening DTE, or increasing Min Bid/LIQ filters.")
        else:
            st.success(f"Found {len(results)} matching puts.")
            show_cols = [
                "provider","option_symbol","underlying","type","strike","expiration","bid","eff_bid","ask",
                "bid_strike_pct","dte","volume","open_interest","underlying_price","updated"
            ]
            show_cols = [c for c in show_cols if c in results.columns]
            st.dataframe(results[show_cols].head(int(max_rows)), use_container_width=True)
            st.download_button("Download CSV", results.to_csv(index=False).encode(), file_name="inflated_puts.csv", mime="text/csv")

    with st.expander("Notes & sanity checks"):
        st.markdown(
            """
- This flags math-based premium richness: **Bid/Strike %**. Always evaluate spreads, IV, borrow, earnings, and assignment risk.
- Use **Min Bid**, **OI**, **Volume** to avoid illiquid/stale quotes.
- Typical windows: **7–21 DTE** for short premium, **30–60 DTE** for broader.
- For earnings crush plays: avoid tickers that ran up pre-event unless you're comfortable with gap risk.
"""
        )


# --- Benzinga News/PRs for a single ticker (today best-effort) ---

def fetch_benzinga_news_for_date(symbol: str, day: pd.Timestamp) -> pd.DataFrame:
    """Best-effort scrape of Benzinga news/press for a ticker on a given date.
    Returns DataFrame columns: [time,title,url,source]."""
    sym = symbol.strip().lower()
    urls = [
        f"https://www.benzinga.com/stock/{sym}",
        f"https://www.benzinga.com/stock/{sym}/news",
        f"https://www.benzinga.com/pressreleases/companies/{symbol.strip().upper()}",
    ]
    rows: list[dict] = []
    import re
    for u in urls:
        try:
            r = requests.get(u, headers=BENZ_NEWS_HEADERS, timeout=30)
            if r.status_code != 200 or not r.text:
                continue
            html = r.text
            for m in re.finditer(r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL):
                href = m.group(1)
                # Require the ticker to appear in the link/title/context to avoid generic category links
                href_l = href.lower()
                if (f"/stock/{sym}" not in href_l) and (f"symbol={sym}" not in href_l) and (sym not in href_l):
                    # we will also check title below; if both miss we skip later
                    pass
                if ("/news/" not in href and 
                    "/pressrelease" not in href and 
                    "/press-releases" not in href and 
                    "/pressreleases/" not in href):
                    continue
                text = re.sub(r"<[^>]+>", " ", m.group(2))
                title = re.sub(r"\s+", " ", text).strip()
                if not title:
                    continue
                around = html[max(0, m.start()-300): m.end()+300]
                dt_match = re.search(r'datetime="([0-9T:\-\+Z]+)"', around)
                ts = dt_match.group(1) if dt_match else None
                title_l = title.lower()
                if (sym not in title_l) and (f"/stock/{sym}" not in href_l) and (sym not in href_l):
                    continue
                rows.append({
                    "time": ts,
                    "title": title,
                    "url": href if href.startswith("http") else ("https://www.benzinga.com" + href),
                    "source": "Benzinga",
                })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["time","title","url","source"])
    df = pd.DataFrame(rows).drop_duplicates()
    # prefer items whose parsed date is today
    df["_date"] = pd.to_datetime(df["time"], errors="coerce").dt.date
    today = pd.to_datetime(day).date()
    todays = df[df["_date"] == today]
    if not todays.empty:
        df = todays.copy()
    df.drop(columns=["_date"], inplace=True, errors="ignore")
    # bubble likely earnings-ish items first
    KEYS = ["earnings","results","revenue","guidance","q1","q2","q3","q4"]
    df["_k"] = df["title"].str.lower().apply(lambda s: any(k in s for k in KEYS))
    df = df.sort_values(["_k","time","title"], ascending=[False, False, True]).drop(columns=["_k"]).reset_index(drop=True)
    return df


with earn_ticker_tab:
    st.header("🔎 Single Ticker Earnings (multi‑source)")
    colx, coly = st.columns([2,1])

    # allow multiple tickers, comma/space/newline separated (case-insensitive)
    def _parse_syms_inline(text: str) -> list[str]:
        raw = (text or "").replace("\n", ",").replace("\t", ",").replace(" ", ",")
        return [s.strip().upper() for s in raw.split(",") if s.strip()]

    with colx:
        q_symbols_text = st.text_input(
            "Enter ticker(s)",
            value="",
            placeholder="e.g., AAPL, MSFT (comma/space/newline separated)",
            key="earn_syms_input",
        )
    with coly:
        if provider_choice == "Polygon" and cred:
            st.caption("Polygon key detected — will include Polygon where available.")
        else:
            st.caption("Uses Yahoo; add Polygon key to include Polygon.")

    run_sym = st.button("Check earnings dates", key="btn_check_earn_dates")

    if run_sym:
        syms = _parse_syms_inline(q_symbols_text)
        if not syms:
            st.warning("Enter at least one ticker (comma/space/newline separated).")
        else:
            all_rows: list[pd.DataFrame] = []
            miss: list[str] = []
            for s in syms:
                df_s = get_symbol_earnings_multi(s, cred if provider_choice == "Polygon" else None)
                if df_s is None or df_s.empty:
                    miss.append(s)
                else:
                    all_rows.append(df_s)

            if all_rows:
                df_sym = pd.concat(all_rows, ignore_index=True)
                try:
                    df_sym = (
                        df_sym
                        .dropna(subset=["date"])  # guard
                        .sort_values(["symbol","date","source"])
                        .drop_duplicates(["symbol","date"])  # one row per symbol/date
                        .reset_index(drop=True)
                    )
                except Exception:
                    pass
                st.dataframe(df_sym, use_container_width=True)
                st.caption("Tip: tickers are normalized to uppercase; you can paste them in any case.")
            else:
                st.warning("No earnings dates found from the current sources.")

            if miss:
                st.warning(f"No earnings dates found from current sources for: {', '.join(miss)}")

        # Benzinga news/press for today — show only when a single symbol is provided to avoid noise
        syms_for_news = _parse_syms_inline(q_symbols_text)
        if len(syms_for_news) == 1:
            one_sym = syms_for_news[0]
            st.subheader("📰 News / Press Releases (Benzinga — today)")
            news_df = fetch_benzinga_news_for_date(one_sym, pd.Timestamp.utcnow().date())
            if news_df.empty:
                st.caption("No Benzinga items detected for today.")
            else:
                st.dataframe(news_df, use_container_width=True)

with earn_cal_tab:
    st.header("🗓️ Earnings Calendar (auto‑fetch)")
    cc1, cc2, cc3, cc4 = st.columns([1,1,1,1])
    with cc1:
        cal_start = st.date_input("Start date", value=pd.Timestamp.utcnow().date(), key="cal_start")
    with cc2:
        cal_end = st.date_input("End date", value=pd.Timestamp.utcnow().date(), key="cal_end")
    with cc3:
        cal_bmo = st.checkbox("Before Market Opens", value=True, key="cal_bmo")
    with cc4:
        cal_amc = st.checkbox("After Market Closes", value=True, key="cal_amc")
    only_opt = st.checkbox("Only show optionable tickers", value=True, key="cal_only_opt")
    sort_by_iv = st.checkbox("Sort by highest Implied Volatility first", value=True, key="cal_sort_iv")
    btn_cal = st.button("Fetch earnings calendar 📅", key="btn_fetch_calendar")

    if btn_cal:
        if cal_end < cal_start:
            st.error("End date must be on/after start date.")
        else:
            df_cal, stats = fetch_calendar_range_multi(pd.to_datetime(cal_start), pd.to_datetime(cal_end))
            if df_cal.empty:
                st.warning("No earnings found for the selected dates from Nasdaq, Yahoo, Benzinga, EarningsWhispers.")
            else:
                st.caption(
                    f"Source counts — Nasdaq: {stats.get('nasdaq',0)} | Yahoo: {stats.get('yahoo',0)} | Benzinga: {stats.get('benzinga',0)} | EarningsWhispers: {stats.get('earningswhispers',0)}"
                )
                # Session filter
                keep = []
                for _, r in df_cal.iterrows():
                    sess = str(r.get("session") or "").upper()
                    # Map session for display and filtering
                    if (sess == "BMO" and not cal_bmo) or (sess == "AMC" and not cal_amc):
                        continue
                    if sess not in {"BMO","AMC"} and not (cal_bmo or cal_amc):
                        continue
                    keep.append(True)
                df_cal = df_cal[keep] if keep else df_cal.iloc[0:0]

                # Optionability annotation (show all; mark confirmed and unknown)
                if not df_cal.empty:
                    prog = st.progress(0, text="Checking optionability…")
                    ann = []
                    for i, (idx, row) in enumerate(df_cal.iterrows(), 1):
                        prog.progress(int(i/len(df_cal)*100), text=f"{row['symbol']}")
                        status, disp = optionability_mark(provider_choice, cred, row["symbol"])
                        r = row.copy()
                        r["symbol_marked"] = disp
                        r["optionable_status"] = status
                        # For display: map session to new label
                        s = str(r.get("session") or "").upper()
                        if s == "BMO":
                            r["session"] = "Before Market Opens"
                        elif s == "AMC":
                            r["session"] = "After Market Closes"
                        elif s == "":
                            r["session"] = ""
                        ann.append(r)
                    prog.empty()
                    df_cal = pd.DataFrame(ann)

                # If the toggle is on, include confirmed optionable and unknown (so you can still review)
                if only_opt and not df_cal.empty:
                    df_cal = df_cal[df_cal["optionable_status"].isin(["yes", "unknown"])]

                # --- attach IV per underlying (compute once per symbol) ---
                # If we just rebuilt from annotations, ensure we have a DataFrame
                if isinstance(df_cal, list):
                    df_cal = pd.DataFrame(df_cal)

                if not df_cal.empty:
                    unique_syms = sorted(df_cal["symbol"].unique())
                    iv_map: dict[str, t.Optional[float]] = {}
                    iv_src: dict[str, str] = {}
                    prog2 = st.progress(0, text="Estimating IV…")
                    for i, s in enumerate(unique_syms, 1):
                        prog2.progress(int(i/len(unique_syms)*100), text=s)
                        iv, src = get_underlying_iv(provider_choice, cred, s)
                        iv_map[s] = iv
                        iv_src[s] = src
                    prog2.empty()
                    df_cal["iv"] = df_cal["symbol"].map(iv_map)
                    df_cal["iv_source"] = df_cal["symbol"].map(iv_src)

                    # Optional sort by highest IV
                    if sort_by_iv:
                        df_cal = df_cal.sort_values(by=["iv","date","symbol"], ascending=[False, True, True], na_position="last").reset_index(drop=True)
                    else:
                        df_cal = df_cal.sort_values(["date","symbol"]).reset_index(drop=True)

                if df_cal.empty:
                    st.warning("No tickers matched the filters (try disabling optionable filter or widening dates).")
                else:
                    st.success(f"Found {len(df_cal)} earnings events.")

                    # Per‑day counts (helps verify the full range)
                    if not df_cal.empty:
                        day_counts = df_cal.groupby("date").size().reset_index(name="events")
                        st.caption("Per‑day events in range:")
                        st.dataframe(day_counts, use_container_width=True, height=160)

                    # format IV for display in the main table
                    show_df = df_cal.copy()
                    if "symbol_marked" in show_df.columns:
                        if "symbol" in show_df.columns:
                            show_df.drop(columns=["symbol"], inplace=True)
                        show_cols = ["symbol_marked"] + [c for c in show_df.columns if c != "symbol_marked"]
                        show_df = show_df[show_cols]
                    if "iv" in show_df.columns:
                        show_df["iv"] = show_df["iv"].apply(lambda x: f"{x*100:.1f}%" if isinstance(x, (int,float)) else "")
                    st.dataframe(show_df, use_container_width=True)
                    st.caption("Legend: '*'=confirmed optionable, '?'=unknown (couldn't verify with current provider/key).")
                    st.download_button("Download earnings calendar CSV", df_cal.to_csv(index=False).encode(), file_name="earnings_calendar.csv", mime="text/csv")

                    # --- Optional: Benzinga news pulse for a symbol/date from the calendar ---
                    with st.expander("📰 Show Benzinga news/press for a symbol in this range"):
                        try:
                            cal_syms = sorted(df_cal["symbol"].unique().tolist())
                        except Exception:
                            cal_syms = []
                        if not cal_syms:
                            st.caption("No symbols to preview.")
                        else:
                            pick_sym = st.selectbox("Symbol", cal_syms, key="cal_news_sym")
                            # collect available dates for that symbol from the current table
                            sym_dates = (
                                pd.to_datetime(df_cal[df_cal["symbol"]==pick_sym]["date"], errors="coerce")
                                  .dt.date.dropna().unique().tolist()
                            )
                            sym_dates = sorted(sym_dates)
                            default_date = sym_dates[0] if sym_dates else pd.Timestamp.utcnow().date()
                            pick_date = st.date_input("News date", value=default_date, key="cal_news_date")
                            if st.button("Fetch Benzinga headlines", key="btn_cal_news"):
                                news_df = fetch_benzinga_news_for_date(pick_sym, pick_date)
                                if news_df.empty:
                                    st.caption("No Benzinga items detected for the selected date.")
                                else:
                                    st.dataframe(news_df, use_container_width=True)

with earn_list_tab:
    st.header("🕒 Earnings with Options — Finder")
    st.caption("Filter an earnings list by date/session and keep only tickers that have listed options (via your selected data provider). Upload a CSV or paste a quick list.")

    colA, colB, colC = st.columns([1,1,1])
    with colA:
        earn_start = st.date_input("Start date", value=pd.Timestamp.utcnow().date(), key="earn_start")
    with colB:
        earn_end = st.date_input("End date", value=pd.Timestamp.utcnow().date(), key="earn_end")
    with colC:
        show_bmo = st.checkbox("Before Market Opens", value=True, key="earn_bmo")
        show_amc = st.checkbox("After Market Closes", value=True, key="earn_amc")

    st.write("**Input options:** Upload a CSV with columns: `symbol,date,session` (session values: `BMO` or `AMC`), or paste tickers and pick a single session for today.")
    e_csv = st.file_uploader("Upload earnings CSV", type=["csv"], key="earn_csv")

    tickers_text = st.text_area("Or paste tickers (comma/space/newline separated)", value="", height=80, key="earn_tickers")
    manual_session = st.selectbox("Session for pasted tickers", ["Before Market Opens","After Market Closes"], index=1, key="earn_session")
    run_earn = st.button("Find earnings with options ✅", key="btn_find_earnings")

    def _parse_syms(text: str) -> list[str]:
        raw = text.replace("\n", ",").replace("\t", ",").replace(" ", ",")
        return sorted({s.strip().upper() for s in raw.split(",") if s.strip()})

    if run_earn:
        rows = []
        if e_csv is not None:
            try:
                df_e = pd.read_csv(io.BytesIO(e_csv.getvalue()))
                cols = {c.lower(): c for c in df_e.columns}
                for required in ["symbol","date","session"]:
                    if required not in [k.lower() for k in df_e.columns]:
                        st.error("CSV must have columns: symbol, date, session (BMO/AMC)")
                        df_e = None
                        break
                if df_e is not None:
                    df_e = df_e.rename(columns={cols.get("symbol"): "symbol", cols.get("date"): "date", cols.get("session"): "session"})
                    df_e["symbol"] = df_e["symbol"].astype(str).str.upper()
                    df_e["date"] = pd.to_datetime(df_e["date"], errors="coerce").dt.date
                    df_e["session"] = df_e["session"].astype(str).str.upper().str.strip()
                    mask_date = (df_e["date"] >= earn_start) & (df_e["date"] <= earn_end)
                    mask_sess = df_e["session"].isin((["BMO"] if show_bmo else []) + (["AMC"] if show_amc else []))
                    df_e = df_e[mask_date & mask_sess]
                    rows = df_e.to_dict("records")
            except Exception as e:
                st.error(f"Earnings CSV error: {e}")
        if not rows:
            syms = _parse_syms(tickers_text)
            if syms:
                today_date = pd.Timestamp.utcnow().date()
                if not (earn_start <= today_date <= earn_end):
                    st.info("Manual list assumes today's date; adjust date range to include today, or upload a CSV with explicit dates.")
                _sess_code = "BMO" if manual_session == "Before Market Opens" else "AMC"
                for s in syms:
                    if (_sess_code == "BMO" and show_bmo) or (_sess_code == "AMC" and show_amc):
                        rows.append({"symbol": s, "date": today_date, "session": _sess_code})

        if not rows:
            st.warning("Provide an earnings CSV or paste some tickers.")
        else:
            ann_rows = []
            prog = st.progress(0, text="Checking optionability…")
            for i, r in enumerate(rows, 1):
                prog.progress(int(i/len(rows)*100), text=f"{r['symbol']}")
                status, disp = optionability_mark(provider_choice, cred, r["symbol"])
                # For display: map session to new label
                sess = str(r.get("session") or "").upper()
                session_label = ""
                if sess == "BMO":
                    session_label = "Before Market Opens"
                elif sess == "AMC":
                    session_label = "After Market Closes"
                else:
                    session_label = sess
                ann_rows.append({
                    "symbol": r["symbol"],
                    "symbol_marked": disp,
                    "optionable_status": status,
                    "date": r["date"],
                    "session": session_label,
                })
            prog.empty()
            earn_df = pd.DataFrame(ann_rows).sort_values(["date","symbol"]) if ann_rows else pd.DataFrame()
            if earn_df.empty:
                st.warning("No tickers after filters.")
            else:
                show_cols = ["symbol_marked","optionable_status","date","session"]
                for c in ["symbol_marked","optionable_status","date","session"]:
                    if c not in earn_df.columns:
                        show_cols = [col for col in show_cols if col != c]
                st.dataframe(earn_df[show_cols], use_container_width=True)
                st.caption("Legend: '*'=confirmed optionable, '?'=unknown (couldn't verify); blank means 'not optionable' per provider.")
                st.download_button("Download earnings-with-options CSV", earn_df.to_csv(index=False).encode(), file_name="earnings_with_options.csv", mime="text/csv")