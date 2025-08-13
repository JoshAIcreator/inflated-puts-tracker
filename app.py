import os
import io
import typing as t
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil import parser as dtp
import streamlit as st

# --- Secrets/env helper ---
def _get_secret(name: str, default: str = "") -> str:
    try:
        # prefer Streamlit Secrets if available (Cloud/local .streamlit/secrets.toml)
        if hasattr(st, "secrets") and name in st.secrets:
            return str(st.secrets.get(name, default))
    except Exception:
        pass
    return os.getenv(name, default)

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

    def _chain(self, symbol: str, expiration: str) -> list[dict]:
        params = {
            "underlying_ticker": symbol,
            "contract_type": "put",
            "expiration_date": expiration,
            "limit": 1000,
            "apiKey": self.api_key,
        }
        r = self.sess.get(f"{self.base}/v3/reference/options/contracts", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        return results

    def _nbbo(self, option_symbol: str) -> dict:
        r = self.sess.get(f"{self.base}/v3/quotes/{option_symbol}/nbbo/latest", params={"apiKey": self.api_key}, timeout=30)
        if r.status_code != 200:
            return {}
        return r.json().get("results", {})

    def _expirations(self, symbol: str) -> list[str]:
        r = self.sess.get(f"{self.base}/v3/reference/options/contracts", params={"underlying_ticker": symbol, "limit": 1000, "apiKey": self.api_key}, timeout=60)
        r.raise_for_status()
        res = r.json().get("results", [])
        exps = sorted({x.get("expiration_date") for x in res if x.get("expiration_date")})
        return exps

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
                contracts = self._chain(symbol, exp)
            except Exception:
                continue
            for c in contracts:
                opt = c.get("ticker") or c.get("options_ticker")
                if not opt:
                    continue
                nbbo = self._nbbo(opt) or {}
                bid = float(nbbo.get("bid_price", 0) or 0)
                ask = float(nbbo.get("ask_price", 0) or 0)
                strike = float(c.get("strike_price", 0) or 0)
                if strike <= 0:
                    continue
                out.append(OptionQuote(
                    provider=self.name,
                    option_symbol=opt,
                    underlying=symbol,
                    type="put",
                    strike=strike,
                    expiration=str(d),
                    bid=bid,
                    ask=ask,
                    last=None,
                    volume=None,
                    open_interest=None,
                    underlying_price=None,
                    exch=None,
                    updated=nbbo.get("sip_timestamp"),
                ))
        return out

# ==========================
# UI
# ==========================

st.set_page_config(page_title="Inflated Put Tracker (Bid/Strike% Scanner)", page_icon="📈", layout="wide")
st.title("📈 Inflated Put Tracker — Bid/Strike% Scanner")

with st.sidebar:
    st.header("Data Source")
    provider_choice = st.selectbox("Provider", ["Tradier", "Polygon", "CSV only"], index=0)
    cred = ""
    if provider_choice == "Tradier":
        cred = st.text_input("Tradier Token", type="password", value=_get_secret("TRADIER_TOKEN", ""))
    elif provider_choice == "Polygon":
        cred = st.text_input("Polygon API Key", type="password", value=_get_secret("POLYGON_KEY", ""))

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
    max_dte = st.number_input("Max DTE (days)", min_value=1, step=1, value=45)
    min_bid = st.number_input("Min Bid ($)", min_value=0.0, step=0.05, value=0.10)
    min_oi = st.number_input("Min Open Interest", min_value=0, step=10, value=50)
    min_vol = st.number_input("Min Volume (today)", min_value=0, step=10, value=0)
    moneyness = st.selectbox("Moneyness (requires underlying price in feed)", ["Any", "OTM only", "ITM only"], index=0)
    max_rows = st.slider("Max rows", min_value=100, max_value=5000, value=1000, step=100)

    uploaded_quotes = st.file_uploader("Or upload an option quotes CSV to filter", type=["csv"])  # optional
    run = st.button("Run scan 🚀")

st.caption("Start with a small list, confirm behavior, then widen to big universes/watchlists.")

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["bid"] = pd.to_numeric(df["bid"], errors="coerce").fillna(0.0)
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce").fillna(0.0)
    df["bid_strike_pct"] = (df["bid"] / df["strike"]) * 100.0
    df["expiration"] = pd.to_datetime(df["expiration"], errors="coerce").dt.date.astype(str)
    df["dte"] = (pd.to_datetime(df["expiration"]) - pd.Timestamp.utcnow().normalize()).dt.days
    return df

def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = compute_metrics(df)
    mask = (
        (df["bid_strike_pct"] >= float(target_pct)) &
        (df["dte"].between(int(min_dte), int(max_dte))) &
        (df["bid"] >= float(min_bid))
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
    out = df[mask].sort_values(["bid_strike_pct", "bid"], ascending=[False, False])
    return out

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
    for idx, sym in enumerate(symbols, 1):
        st.progress(int(idx / max(1, len(symbols)) * 100), text=f"Fetching {sym} ({idx}/{len(symbols)})")
        try:
            rows.extend(provider.get_put_quotes(sym, int(min_dte), int(max_dte)))
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
    return df

results = None
if uploaded_quotes is not None:
    try:
        df = pd.read_csv(io.BytesIO(uploaded_quotes.getvalue()))
        # normalize column names
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
            results = filter_rows(live_df)

if results is not None:
    if results.empty:
        st.warning("No matches with current filters. Try lowering Target %, widening DTE, or increasing Min Bid/LIQ filters.")
    else:
        st.success(f"Found {len(results)} matching puts.")
        show_cols = [
            "provider","option_symbol","underlying","type","strike","expiration","bid","ask",
            "bid_strike_pct","dte","volume","open_interest","underlying_price","updated"
        ]
        show_cols = [c for c in show_cols if c in results.columns]
        st.dataframe(results[show_cols].head(int(max_rows)), use_container_width=True)
        st.download_button("Download CSV", results.to_csv(index=False).encode(), file_name="inflated_puts.csv", mime="text/csv")

with st.expander("Notes & sanity checks"):
    st.markdown("""
- This flags math-based premium richness: **Bid/Strike %**. Always evaluate spreads, IV, borrow, earnings, and assignment risk.
- Use **Min Bid**, **OI**, **Volume** to avoid illiquid/stale quotes.
- Typical windows: **7–21 DTE** for short premium, **30–60 DTE** for broader.
- For earnings crush plays: avoid tickers that ran up pre-event unless you're comfortable with gap risk.
""")