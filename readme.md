```markdown
# 🧠 Stock Price ETL & Forecasting with Airflow + Snowflake

This project demonstrates an end-to-end ELT + ML pipeline using **Apache Airflow**, **Snowflake**, and **yFinance** data. It is composed of two Airflow DAGs:

- [`yf_stock_price_etl_backup`](dags/yf_stock_price_etl_backup.py) — Extracts stock OHLCV data from Yahoo Finance and loads it into Snowflake.
- [`sf_ml_forcast_dag`](dags/sf_ml_forcast_dag.py) — Trains a time series forecasting model using Snowflake ML and stores actual + forecasted values.

---

## 🧩 Architecture Overview

```

yFinance → Airflow → Snowflake (RAW → TRANSFORM → ANALYTICS)
↘
Trigger ML DAG → Snowflake ML Forecast

```

---

## 1️⃣ DAG: `yf_stock_price_etl_backup`

### Description

Extracts daily OHLCV (Open, High, Low, Close, Volume) stock price data for selected tickers using [yFinance](https://pypi.org/project/yfinance/), transforms the data, and loads it into a Snowflake table.

### Steps

1. **Extract**  
   Pulls the last N days of data for selected tickers from Yahoo Finance.

2. **Transform**  
   Cleans and formats the data for Snowflake ingestion.

3. **Load**  
   Loads data into:  
```

<DATABASE>.RAW.STOCK_PRICE

```

4. **Trigger**  
Triggers the next DAG (`sf_ml_forcast_dag`) after successful load.

---

## 2️⃣ DAG: `sf_ml_forcast_dag`

### Description

Uses **Snowflake ML Forecasting** to predict future stock closing prices for each symbol and merges the forecast with actual data into an analytics table.

### Steps

1. **Train Model**  
Creates/refreshes Snowflake ML forecast object based on:
```

<DATABASE>.TRANSFORM.STOCK_PRICE

````

2. **Forecast and Union**  
- Calls the forecast for the next N days.
- Saves the result in:
  ```
  <DATABASE>.TRANSFORM.MARKET_DATA_FORECAST
  ```
- Unions forecast with actuals into:
  ```
  <DATABASE>.ANALYTICS.MARKET_DATA
  ```

---

## ⚙️ Airflow Variables

Ensure the following Airflow Variables are set:

| Variable Name               | Description                                 | Example Value              |
|----------------------------|---------------------------------------------|----------------------------|
| `snowflake_conn_id`        | Airflow Connection ID for Snowflake         | `snowflake_conn`           |
| `snowflake_database`       | Target Snowflake database                   | `USER_DB_HORNET`           |
| `yf_symbols`               | Comma-separated list of tickers             | `AAPL,MSFT,GOOGL,AMZN`     |
| `yf_days_back`             | Number of historical days to fetch          | `180`                      |
| `forecast_horizon_days`    | Days to forecast in the future              | `7`                        |

---

## 🧠 Snowflake Structure

| Layer      | Object Name                              | Description                           |
|------------|-------------------------------------------|---------------------------------------|
| RAW        | `RAW.STOCK_PRICE`                         | Raw historical stock data             |
| TRANSFORM  | `TRANSFORM.STOCK_PRICE`                   | Cleaned & validated stock data        |
| TRANSFORM  | `TRANSFORM.MARKET_DATA_FORECAST`          | Output of Snowflake ML forecast       |
| ANALYTICS  | `ANALYTICS.MARKET_DATA`                   | Combined actual + forecasted results  |
| ANALYTICS  | `ANALYTICS.PREDICT_STOCK_PRICE` (ML)      | Forecast model per ticker             |

---

## 🗓️ Scheduling

- `yf_stock_price_etl_backup` runs daily at **03:15 UTC**
- `sf_ml_forcast_dag` runs daily at **03:45 UTC** (after ETL)

---

## 📦 Requirements

- Apache Airflow with the following providers:
- `apache-airflow-providers-snowflake`
- `yfinance`
- Valid Snowflake account and connection set in Airflow
- DAGs should be placed under the `dags/` directory in your Airflow project

---

## ✅ Example Use Case

Use this project to:

- Automate ingestion of public market data
- Train and monitor forecasting models in Snowflake
- Visualize actual vs predicted prices for portfolio stocks

---

## 🔒 Security Notes

- Never store credentials in code.
- Use Airflow's **Connection** and **Variable** features to securely manage sensitive information.

---

## 📁 Project Structure

````

dags/
├── yf_stock_price_etl_backup.py       # ETL DAG
└── sf_ml_forcast_dag.py               # ML Forecast DAG
README.md                              # Project documentation

```

---

## 📈 Sample Output

| SYMBOL | DATE       | ACTUAL | FORECAST | LOWER_BOUND | UPPER_BOUND |
|--------|------------|--------|----------|--------------|--------------|
| AAPL   | 2025-10-01 | 170.12 | NULL     | NULL         | NULL         |
| AAPL   | 2025-10-07 | NULL   | 172.84   | 168.91       | 176.22       |

---

## 🧪 Future Improvements

- Add SLA alerts on ETL failure
- Backfill support for missing dates
- Add transformations before modeling (e.g., outlier removal)
- Integration with dashboards (e.g., Streamlit or Power BI)

---

## 👨‍💻 Author

Created by [Your Name or Team Name]  
License: MIT or your preferred license
```

---

Let me know if you want a version with inline images or diagrams, or if you'd like to break this into multiple docs (e.g. `/docs/etl.md`, `/docs/forecast.md`).
