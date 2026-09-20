import os
import json
import time
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def classify_filing_signal(form_type):
    if form_type in ['10-K', '10-K/A']:
        return "ANNUAL_AUDITED_FINANCIALS"
    elif form_type in ['10-Q', '10-Q/A']:
        return "QUARTERLY_EARNINGS_DISCLOSURE"
    elif form_type in ['8-K', '8-K/A']:
        return "MATERIAL_CORPORATE_EVENT"
    elif form_type in ['4', '4/A']:
        return "INSIDER_OWNERSHIP_TRANSACTION"
    elif form_type in ['13D', '13G']:
        return "BENEFICIAL_OWNERSHIP_STAKE"
    return "REGULATORY_DISCLOSURE"

def main():
    print("[KRAKEN BACKFILL] Initializing 5-Year SEC EDGAR Historical Pipeline...")

    # 1. BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.kraken_filings"

    # 2. SEC Requirements: SEC strictly mandates a declared User-Agent
    headers = {
        "User-Agent": "HoloEarthEnterpriseAnalytics ResearchOps@holoearth.internal",
        "Accept-Encoding": "gzip, deflate"
    }

    # 3. Target Universe with SEC Central Index Keys (CIKs)
    # Core defense, enterprise tech, logistics, and mega-cap bellwethers
    target_entities = {
        "0000936468": {"ticker": "LMT", "name": "Lockheed Martin"},
        "0000012927": {"ticker": "BA", "name": "Boeing"},
        "0000040533": {"ticker": "GD", "name": "General Dynamics"},
        "0001133421": {"ticker": "NOC", "name": "Northrop Grumman"},
        "0001321655": {"ticker": "PLTR", "name": "Palantir Technologies"},
        "0000789019": {"ticker": "MSFT", "name": "Microsoft Corp"},
        "0001018724": {"ticker": "AMZN", "name": "Amazon.com Inc"},
        "0001045810": {"ticker": "NVDA", "name": "Nvidia Corp"},
        "0000320193": {"ticker": "AAPL", "name": "Apple Inc"},
        "0001652044": {"ticker": "GOOGL", "name": "Alphabet Inc"},
        "0001318605": {"ticker": "TSLA", "name": "Tesla Inc"},
        "0001616707": {"ticker": "ZIM", "name": "ZIM Integrated Shipping"},
        "000006769":  {"ticker": "MATX", "name": "Matson Inc"}
    }

    cutoff_date = (datetime.utcnow() - timedelta(days=10*365)).strftime('%Y-%m-%d')
    timestamp_iso = datetime.utcnow().isoformat()
    all_filings = []

    # 4. SEC Submissions API Extraction
    for cik, meta in target_entities.items():
        padded_cik = cik.zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
        
        print(f"[KRAKEN] Fetching SEC master history for {meta['ticker']} ({meta['name']})...")
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                recent = data.get("filings", {}).get("recent", {})
                
                forms = recent.get("form", [])
                filing_dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])
                descriptions = recent.get("primaryDocDescription", [])
                report_dates = recent.get("reportDate", [])

                total_entries = len(forms)
                for idx in range(total_entries):
                    f_date = filing_dates[idx] if idx < len(filing_dates) else ""
                    
                    # 5-year chronological filter
                    if f_date and f_date >= cutoff_date:
                        form_name = forms[idx]
                        
                        # Filter to high-conviction decision forms
                        if form_name in ['8-K', '8-K/A', '10-K', '10-K/A', '10-Q', '10-Q/A', '4', '4/A']:
                            desc = descriptions[idx] if (idx < len(descriptions) and descriptions[idx]) else form_name
                            acc_clean = accessions[idx].replace("-", "") if idx < len(accessions) else ""
                            doc_link = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/"

                            all_filings.append({
                                "timestamp": timestamp_iso,
                                "domain": "KRAKEN",
                                "entity_id": meta['ticker'],
                                "signal_type": classify_filing_signal(form_name),
                                "form_type": form_name,
                                "filing_date": f_date,
                                "report_date": report_dates[idx] if idx < len(report_dates) else f_date,
                                "accession_number": accessions[idx] if idx < len(accessions) else "UNKNOWN",
                                "filing_url": doc_link,
                                "description": str(desc)[:200]
                            })
            else:
                print(f"[KRAKEN ERROR] SEC rejected request for {meta['ticker']}: HTTP {response.status_code}")
        except Exception as e:
            print(f"[KRAKEN ERROR] Request failed for {meta['ticker']}: {e}")

        # SEC mandates max 10 requests/second rate limit
        time.sleep(0.2)

    total_records = len(all_filings)
    print(f"[KRAKEN] Extracted {total_records} historical filings over 5 years.")

    # 5. Ingestion to BigQuery
    if total_records > 0:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            ignore_unknown_values=True,
            autodetect=True
        )

        try:
            job = client.load_table_from_json(all_filings, table_id, job_config=job_config)
            job.result()
            print(f"[KRAKEN] Successfully loaded {total_records} records into {table_id}.")
        except Exception as e:
            print(f"[KRAKEN ERROR] BigQuery load failed: {e}")
    else:
        print("[KRAKEN] No matching records retrieved.")

if __name__ == "__main__":
    main()
  
