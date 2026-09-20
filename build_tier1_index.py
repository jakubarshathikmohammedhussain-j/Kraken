import io
import json
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def build_tier1_master():
    print("[HOLO_EARTH] Ingesting Tier 1 Master Index (S&P 500 + Primes)...")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "HoloEarthAnalytics Admin@holoearthdata.org"}
    resp = requests.get(url, headers=headers)
    tables = pd.read_html(io.StringIO(resp.text))
    sp500_df = tables[0]

    sp500_df = sp500_df.rename(columns={
        "Symbol": "ticker",
        "Security": "company_name",
        "GICS Sector": "gics_sector",
        "GICS Sub-Industry": "gics_sub_industry",
        "CIK": "cik"
    })

    sp500_df["ticker"] = sp500_df["ticker"].str.replace(".", "-", regex=False)
    sp500_df["cik"] = sp500_df["cik"].astype(str).str.zfill(10)

    # Core prime additions if not already present
    extra_primes = [
        {"ticker": "PLTR", "company_name": "Palantir Technologies Inc.", "cik": "0001321655", "gics_sector": "Information Technology", "gics_sub_industry": "Systems Software"},
        {"ticker": "ZIM", "company_name": "ZIM Integrated Shipping Services", "cik": "0001616707", "gics_sector": "Industrials", "gics_sub_industry": "Marine Transportation"}
    ]

    for prime in extra_primes:
        if prime["ticker"] not in sp500_df["ticker"].values:
            sp500_df = pd.concat([sp500_df, pd.DataFrame([prime])], ignore_index=True)

    records = sp500_df[["ticker", "company_name", "cik", "gics_sector", "gics_sub_industry"]].to_dict(orient="records")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.tier1_master_index"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    job = client.load_table_from_json(records, table_id, job_config=job_config)
    job.result()
    print(f"[HOLO_EARTH] Successfully established {len(records)} Tier 1 constituents in {table_id}.")

if __name__ == "__main__":
    build_tier1_master()
  
