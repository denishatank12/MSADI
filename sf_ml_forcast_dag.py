from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Config
SNOWFLAKE_CONN_ID = Variable.get("snowflake_conn_id", default_var="snowflake_conn")
DATABASE = Variable.get("snowflake_database", default_var="USER_DB_HORNET")
FORECAST_HORIZON = int(Variable.get("forecast_horizon_days", default_var="7"))

def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()

@task
def train_model():
    """Create/replace ML Forecast on TRANSFORM.STOCK_PRICE (multi-symbol)."""
    conn = get_snowflake_conn(); cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        # Ensure schemas exist
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.TRANSFORM;")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.ANALYTICS;")

        # View for ML input
        cur.execute(f"""
            CREATE OR REPLACE VIEW {DATABASE}.TRANSFORM.V_STOCK_FOR_ML AS
            SELECT DATE, CLOSE, SYMBOL
            FROM {DATABASE}.TRANSFORM.STOCK_PRICE;
        """)

        # Forecast object
        cur.execute(f"""
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {DATABASE}.ANALYTICS.PREDICT_STOCK_PRICE (
              INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{DATABASE}.TRANSFORM.V_STOCK_FOR_ML'),
              SERIES_COLNAME => 'SYMBOL',
              TIMESTAMP_COLNAME => 'DATE',
              TARGET_COLNAME => 'CLOSE',
              CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
            );
        """)
        cur.execute("COMMIT;")
    except Exception:
        try: cur.execute("ROLLBACK;")
        finally: ...
        raise
    finally:
        cur.close(); conn.close()

@task
def forecast_and_union():
    """Forecast N days and union with actuals into ANALYTICS.MARKET_DATA."""
    conn = get_snowflake_conn(); cur = conn.cursor()
    try:
        cur.execute("BEGIN;")
        fn = f"{DATABASE}.ANALYTICS.PREDICT_STOCK_PRICE"
        fc_tbl = f"{DATABASE}.TRANSFORM.MARKET_DATA_FORECAST"
        final_tbl = f"{DATABASE}.ANALYTICS.MARKET_DATA"

        # Run forecast
        cur.execute(f"""
            CALL {fn}!FORECAST(
              FORECASTING_PERIODS => {FORECAST_HORIZON},
              CONFIG_OBJECT => {{ 'prediction_interval': 0.95 }}
            );
        """)
        call_query_id = cur.sfqid  # <-- query id of the CALL

        # Capture result set into a table
        cur.execute(f"""
            CREATE OR REPLACE TABLE {fc_tbl} AS
            SELECT *
            FROM TABLE(RESULT_SCAN('{call_query_id}'));
        """)

        # Build final union table (actuals + forecast)
        cur.execute(f"""
            CREATE OR REPLACE TABLE {final_tbl} AS
            SELECT
              SYMBOL,
              DATE,
              CLOSE AS ACTUAL,
              NULL::FLOAT AS FORECAST,
              NULL::FLOAT AS LOWER_BOUND,
              NULL::FLOAT AS UPPER_BOUND
            FROM {DATABASE}.TRANSFORM.STOCK_PRICE
            UNION ALL
            SELECT
              TO_VARCHAR(SERIES) AS SYMBOL,
              CAST(TS AS DATE)   AS DATE,
              NULL::FLOAT        AS ACTUAL,
              FORECAST,
              LOWER_BOUND,
              UPPER_BOUND
            FROM {fc_tbl};
        """)
        cur.execute("COMMIT;")
    except Exception:
        try: cur.execute("ROLLBACK;")
        finally: ...
        raise
    finally:
        cur.close(); conn.close()

with DAG(
    dag_id="sf_ml_forcast_dag",
    start_date=datetime(2025, 10, 1),
    schedule="45 3 * * *",  # after ETL
    catchup=False,
    tags=["ELT", "forecast", "snowflake"],
) as dag:
    a = train_model()
    b = forecast_and_union()
    a >> b
