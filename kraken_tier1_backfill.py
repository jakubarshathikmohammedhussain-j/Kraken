import os
import json
import time
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def classify_form(form):
    if form in ['10-K', '10-K/A']:
        return "ANNUAL_AUDITED_REPORT"
    elif form in ['10-Q', '10-Q/A']:
        return "QUARTERLY_DISCLOSURE"
    elif form in ['8-K', '8-K/A']:
        return "MATERIAL_EVENT"
    elif form in ['4', '4/A']:
        return "INSIDER_TRANSACTION"
    return "REGULATORY_DISCLOSURE"

def main():
    print("[KRAKEN TIER 1] Initializing 10-Year SEC Ingestion across all Tier 1 Assets...")

    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    
    # 1. Fetch all CIKs directly from Master Dimension
    query = f"""
        SELECT ticker, cik, company_name 
        FROM `{creds_dict['project_id']}.telemetry_bronze.tier1_master_index`
        WHERE cik IS NOT NULL
    """
    targets = client.query(query).to_dataframe().to_dict(orient="records")
    print(f"[KRAKEN] Loaded {len(targets)} targets from tier1_master_index.")

    headers = {
        "User-Agent": "HoloEarthAnalytics EnterpriseOps@holoearthdata.org",
        "Accept-Encoding": "gzip, deflate"
    }

    cutoff_date = (datetime.utcnow() - timedelta(days=10*365)).strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()
    all_filings = []
    chunk_size = 25000
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.kraken_filings"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True
    )

    # 2. Iterate through all CIKs with rate-limiting protection
    for i, target in enumerate(targets):
        cik = str(target['cik']).zfill(10)
        ticker = target['ticker']
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"

        try:
            resp = requests.get(url, headers=headers, timeout=12)
            if resp.status_code == 200:
                data = resp.json()
                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])
                descs = recent.get("primaryDocDescription", [])
                reports = recent.get("reportDate", [])

                for idx in range(len(forms)):
                    f_date = dates[idx] if idx < len(dates) else ""
                    if f_date and f_date >= cutoff_date:
                        f_name = forms[idx]
                        if f_name in ['10-K', '10-K/A', '10-Q', '10-Q/A', '8-K', '8-K/A', '4', '4/A']:
                            acc = accessions[idx].replace("-", "") if idx < len(accessions) else ""
                            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/"
                            desc_val = descs[idx] if (idx < len(descs) and descs[idx]) else f_name

                            all_filings.append({
                                "timestamp": timestamp_iso,
                                "domain": "KRAKEN",
                                "entity_id": ticker,
                                "signal_type": classify_form(f_name),
                                "form_type": f_name,
                                "filing_date": f_date,
                                "report_date": reports[idx] if idx < len(reports) else f_date,
                                "accession_number": accessions[idx] if idx < len(accessions) else "UNKNOWN",
                                "filing_url": doc_url,
                                "description": str(desc_val)[:200]
                            })
            elif resp.status_code == 429:
                time.sleep(5)
        except Exception:
            pass

        # Throttle to max 8 requests/sec (SEC allows up to 10)
        time.sleep(0.13)

        # Ingest every 25,000 records to keep VM memory minimal
        if len(all_filings) >= chunk_size:
            client.load_table_from_json(all_filings, table_id, job_config=job_config).result()
            print(f"[KRAKEN] Ingested batch of {len(all_filings)} records. Completed {i + 1}/{len(targets)} entities.")
            all_filings = []

    # Final flush
    if all_filings:
        client.load_table_from_json(all_filings, table_id, job_config=job_config).result()
        print(f"[KRAKEN] Ingested final batch of {len(all_filings)} records.")

    print("[KRAKEN TIER 1] Ingestion complete.")

if __name__ == "__main__":
    main()
    
