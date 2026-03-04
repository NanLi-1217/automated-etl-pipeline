import sys
import sqlite3
from datetime import datetime, timedelta, date

import requests
import pandas as pd
import yfinance as yf


DB_PATH = "procurement_costs.db"
FX_SERIES = "FXUSDCAD"
COPPER_TICKER = "HG=F"


# ----------------------------
# Helpers
# ----------------------------
def get_last_loaded_date(conn: sqlite3.Connection) -> date | None:
    """Return the max(Date) in daily_copper_costs as a python date, or None if table empty."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_copper_costs (
            Date TEXT PRIMARY KEY,
            Copper_Price_USD REAL,
            USD_to_CAD REAL,
            Copper_Price_CAD REAL
        )
    """)
    conn.commit()

    cur.execute("SELECT MAX(Date) FROM daily_copper_costs")
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return datetime.strptime(row[0], "%Y-%m-%d").date()


def log_run(conn: sqlite3.Connection, start_ts: str, end_ts: str, status: str,
            rows_read_fx: int, rows_read_copper: int, rows_written: int, error: str | None):
    """Insert a row into run_log table."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS run_log (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_ts TEXT,
            end_ts TEXT,
            status TEXT,
            rows_read_fx INTEGER,
            rows_read_copper INTEGER,
            rows_written INTEGER,
            error_message TEXT
        )
    """)
    cur.execute("""
        INSERT INTO run_log (start_ts, end_ts, status, rows_read_fx, rows_read_copper, rows_written, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (start_ts, end_ts, status, rows_read_fx, rows_read_copper, rows_written, error))
    conn.commit()


def safe_request_json(url: str, timeout_sec: int = 10) -> dict:
    """GET a URL and parse JSON with basic safety checks."""
    try:
        resp = requests.get(url, timeout=timeout_sec)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout as e:
        raise RuntimeError(f"Request timed out after {timeout_sec}s: {url}") from e
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"HTTP error {resp.status_code} for URL: {url}") from e
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Request failed for URL: {url}") from e
    except ValueError as e:
        raise RuntimeError(f"Invalid JSON response for URL: {url}") from e


# ----------------------------
# Extract
# ----------------------------
def extract_fx_usdcad(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch FX USDCAD from Bank of Canada for [start_date, end_date].
    """
    url = (
        f"https://www.bankofcanada.ca/valet/observations/{FX_SERIES}/json"
        f"?start_date={start_date:%Y-%m-%d}&end_date={end_date:%Y-%m-%d}"
    )
    print(f"[Extract] Fetching FX from BoC: {start_date} → {end_date}")
    data = safe_request_json(url)
    observations = data.get("observations", [])

    if not observations:
        return pd.DataFrame(columns=["Date", "USD_to_CAD"])

    df = pd.DataFrame(observations)

    # Normalize nested dict {'v': '1.3684'} -> float
    df[FX_SERIES] = df[FX_SERIES].apply(lambda x: float(x["v"]) if isinstance(x, dict) and x.get("v") else None)
    df = df.rename(columns={"d": "Date", FX_SERIES: "USD_to_CAD"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()

    return df[["Date", "USD_to_CAD"]]


def extract_copper_close(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch copper futures close prices from Yahoo Finance for [start_date, end_date].
    """
    print(f"[Extract] Fetching Copper close from Yahoo: {start_date} → {end_date}")
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1)

    hist = yf.Ticker(COPPER_TICKER).history(start=start_dt, end=end_dt)
    if hist.empty:
        return pd.DataFrame(columns=["Date", "Copper_Price_USD"])

    df = hist[["Close"]].copy()
    df = df.rename(columns={"Close": "Copper_Price_USD"})

    # Normalize timezone + keep date only
    df.index = df.index.tz_localize(None).normalize()
    df = df.reset_index().rename(columns={"index": "Date"})

    return df[["Date", "Copper_Price_USD"]]


# ----------------------------
# Transform + DQ
# ----------------------------
def apply_business_rules_and_merge(df_copper: pd.DataFrame, df_fx: pd.DataFrame) -> pd.DataFrame:
    # Handle case where both might be empty (e.g., long holidays)
    if df_copper.empty and df_fx.empty:
        return pd.DataFrame()

    # Outer join to keep all dates
    df = pd.merge(df_copper, df_fx, on="Date", how="outer")
    df = df.sort_values("Date")

    # Forward fill only numeric columns (not Date)
    for col in ["Copper_Price_USD", "USD_to_CAD"]:
        if col in df.columns:
            df[col] = df[col].ffill()

    # Drop rows where we still don't have both values (usually the first 1-2 rows of the sliding window)
    df = df.dropna(subset=["Copper_Price_USD", "USD_to_CAD"])

    if df.empty:
        return df

    # Business computation
    df["Copper_Price_CAD"] = df["Copper_Price_USD"] * df["USD_to_CAD"]

    # Round for presentation
    df = df.round(4)  # keep a bit more precision in DB; Power BI can format
    return df


def run_data_quality_checks(df: pd.DataFrame) -> bool:
    """Return True if DQ passes and data should be loaded, False if empty/skip."""
    if df.empty:
        print("[DQ] Notice: Final dataframe is empty after dropping nulls. Likely a weekend/holiday. Skipping load.")
        return False

    # Date uniqueness
    if df["Date"].duplicated().any():
        dupes = df[df["Date"].duplicated(keep=False)].sort_values("Date").head(5)
        raise RuntimeError(f"DQ failed: duplicate Date values found. Examples:\n{dupes}")

    # Non-null
    if df[["Copper_Price_USD", "USD_to_CAD", "Copper_Price_CAD"]].isna().any().any():
        raise RuntimeError("DQ failed: null values exist in required numeric columns.")

    # Basic range sanity (wide thresholds; just prevent nonsense)
    if (df["USD_to_CAD"] <= 0).any() or (df["USD_to_CAD"] > 10).any():
        raise RuntimeError("DQ failed: USD_to_CAD out of expected range (>0 and <=10).")
    if (df["Copper_Price_USD"] <= 0).any() or (df["Copper_Price_USD"] > 1_000).any():
        raise RuntimeError("DQ failed: Copper_Price_USD out of expected range (>0 and <=1000).")

    # Freshness: latest date should not be too old
    latest = df["Date"].max().date()
    if (date.today() - latest).days > 7:
        raise RuntimeError(f"DQ failed: latest data ({latest}) is older than 7 days.")

    return True


# ----------------------------
# Load (Upsert)
# ----------------------------
def upsert_into_sqlite(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_copper_costs (
            Date TEXT PRIMARY KEY,
            Copper_Price_USD REAL,
            USD_to_CAD REAL,
            Copper_Price_CAD REAL
        )
    """)
    conn.commit()

    # Convert Date to string for SQLite
    df_to_load = df.copy()
    df_to_load["Date"] = df_to_load["Date"].dt.strftime("%Y-%m-%d")

    records = df_to_load[["Date", "Copper_Price_USD", "USD_to_CAD", "Copper_Price_CAD"]].values.tolist()

    # Standard upsert (Idempotent operation)
    cur.executemany("""
        INSERT INTO daily_copper_costs (Date, Copper_Price_USD, USD_to_CAD, Copper_Price_CAD)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(Date) DO UPDATE SET
            Copper_Price_USD=excluded.Copper_Price_USD,
            USD_to_CAD=excluded.USD_to_CAD,
            Copper_Price_CAD=excluded.Copper_Price_CAD
    """, records)

    conn.commit()
    return len(records)


# ----------------------------
# Main
# ----------------------------
def main():
    start_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    status = "SUCCESS"
    err_msg = None

    rows_fx = 0
    rows_copper = 0
    rows_written = 0

    conn = sqlite3.connect(DB_PATH)

    try:
        # [MODIFIED]: Always use a sliding window of 10 days.
        # This guarantees ffill() has historical data to anchor on, 
        # and combined with the UPSERT logic, it is 100% safe to re-run.
        start_date = date.today() - timedelta(days=10)
        end_date = date.today()
        
        print(f"[Info] Running ETL for sliding window: {start_date} to {end_date}")

        # Extract
        df_fx = extract_fx_usdcad(start_date, end_date)
        df_copper = extract_copper_close(start_date, end_date)
        rows_fx = len(df_fx)
        rows_copper = len(df_copper)

        print(f"[Extract] FX rows: {rows_fx}, Copper rows: {rows_copper}")

        # Transform
        df_final = apply_business_rules_and_merge(df_copper, df_fx)

        # DQ & Load
        if run_data_quality_checks(df_final):
            rows_written = upsert_into_sqlite(conn, df_final)
            print("\n[Result] Latest rows updated in DB:")
            print(df_final.sort_values("Date").tail(5))
        else:
            status = "SKIPPED_EMPTY"
            print("\n[Result] No new valid business day data to write today.")

        end_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        log_run(conn, start_ts, end_ts, status, rows_fx, rows_copper, rows_written, None)
        print(f"\n✅ Done. Upserted {rows_written} rows into {DB_PATH} (table: daily_copper_costs).")

    except Exception as e:
        status = "FAILED"
        err_msg = str(e)
        end_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        log_run(conn, start_ts, end_ts, status, rows_fx, rows_copper, rows_written, err_msg)
        print(f"\n❌ Pipeline failed: {err_msg}", file=sys.stderr)
        conn.close()
        raise

    conn.close()


if __name__ == "__main__":
    main()