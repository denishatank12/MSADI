from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import pandas as pd
import yfinance as yf

# Connection and database config from Airflow Variables
SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_conn")
DATABASE = Variable.get("snowflake_database", default_var="USER_DB_HORNET")

# Get Stock symbols and days back from Airflow Variables
YF_SYMBOLS = Variable.get("yf_symbols", default_var="AAPL,MSFT,GOOGL,AMZN")
DAYS_BACK = int(Variable.get("yf_days_back", default_var="180"))

def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

@task
def extract_data(symbols_csv: str, days_back: int):
    """
    Pull last N days OHLCV for multiple symbols from yfinance using 'period'.
    Ensure all values are JSON-serializable for XCom (especially Date).
    """
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    period_str = f"{max(days_back, 1)}d"

    out = []
    for sym in symbols:
        df = yf.download(
            sym,
            period=period_str,
            interval="1d",
            auto_adjust=False,
            group_by="column",
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            print(f"No data from yfinance for {sym}, skipping")
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

        needed = ["Open", "High", "Low", "Close", "Volume"]
        if not set(needed).issubset(df.columns):
            print(f"{sym}: missing columns, have {list(df.columns)}; skipping")
            continue

        df = df.reset_index()  # brings index to 'Date'
        # -> Make Date JSON-safe (string) to avoid Timestamp in XCom
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

        for r in df.to_dict(orient="records"):
            r["SYMBOL"] = sym
            out.append(r)

    if not out:
        raise ValueError("No data extracted for any symbol")
    return out  # all primitives (str/float/int), XCom-safe

@task
def transform_data(raw_rows):
    """Type/validate rows -> (symbol, date_str, open, close, high, low, volume)."""
    records = []
    for r in raw_rows:
        try:
            # Date already a string 'YYYY-MM-DD' from extract; keep as string for Snowflake
            d_str = r["Date"]
            records.append((
                r["SYMBOL"],
                d_str,
                float(r["Open"]),
                float(r["Close"]),
                float(r["High"]),
                float(r["Low"]),
                int(r["Volume"]),
            ))
        except Exception as e:
            print(f"Skip row: {e}")
    if not records:
        raise ValueError("No records after transform")
    return records

@task
def load_raw(records):
    """Full refresh into RAW.STOCK_PRICE (transaction)."""
    tgt = f"{DATABASE}.RAW.STOCK_PRICE"
    conn = get_snowflake_conn(); cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.RAW;")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {tgt} (
              SYMBOL VARCHAR(20) NOT NULL,
              DATE   DATE NOT NULL,
              OPEN   DECIMAL(12,4) NOT NULL,
              CLOSE  DECIMAL(12,4) NOT NULL,
              HIGH   DECIMAL(12,4) NOT NULL,
              LOW    DECIMAL(12,4) NOT NULL,
              VOLUME BIGINT NOT NULL,
              PRIMARY KEY (SYMBOL, DATE)
            );
        """)
        cur.execute(f"DELETE FROM {tgt};")
        # DATE comes as 'YYYY-MM-DD' string; Snowflake will cast to DATE implicitly
        cur.executemany(
            f"INSERT INTO {tgt} (SYMBOL, DATE, OPEN, CLOSE, HIGH, LOW, VOLUME) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            records
        )
        cur.execute("COMMIT;")
        print(f"Loaded {len(records)} rows into {tgt}")
    except Exception:
        try:
            cur.execute("ROLLBACK;")
        finally:
            pass
        raise
    finally:
        cur.close(); conn.close()

with DAG(
    dag_id="yf_stock_price_etl_backup",
    start_date=datetime(2025, 10, 1),
    schedule="15 3 * * *",
    catchup=False,
    tags=["y_finance_etl"],
) as dag:
    raw = extract_data(YF_SYMBOLS, DAYS_BACK)
    transformed = transform_data(raw)
    load_ok = load_raw(transformed)

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_sf_ml_forcast_dag",
        trigger_dag_id="sf_ml_forcast_dag",
        wait_for_completion=False,
    )

    load_ok >> trigger_next

