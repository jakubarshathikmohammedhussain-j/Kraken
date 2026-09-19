import os
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def main():
    print("[KRAKEN Node] Initializing SEC EDGAR live feed extraction...")
    
    # BigQuery Setup
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = bigquery.Client(credentials=credentials, project=creds_dict['project_id'])
    
    # Dedicated table for KRAKEN
    table_id = f"{creds_dict['project_id']}.telemetry_bronze.kraken_edgar"
    
    # SEC EDGAR ATOM Feed (Latest Filings)
    url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&owner=include&start=0&count=100&output=atom'
    
    # CRITICAL: The SEC will block your IP if the User-Agent does not declare a name/email
    headers = {
        'User-Agent': 'HoloEarth_Terminal automation@holoearth.system'
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"[KRAKEN ERROR] SEC API rejected request: {response.status_code}")
        return
        
    root = ET.fromstring(response.content)
    namespace = {'atom': 'http://www.w3.org/2005/Atom'}
    
    # Target high-impact market catalysts
    target_forms = ['8-K', '10-K', '10-Q', '4']
    bq_payload = []
    timestamp_iso = datetime.utcnow().isoformat()
    
    for entry in root.findall('atom:entry', namespace):
        title = entry.find('atom:title', namespace).text
        # SEC title format: "8-K - COMPANY NAME (0001234567) (Reporting)"
        form_type = title.split(' - ')[0]
        
        if form_type in target_forms:
            link = entry.find('atom:link', namespace).attrib['href']
            updated = entry.find('atom:updated', namespace).text
            
            bq_payload.append({
                "timestamp": timestamp_iso,
                "domain": "KRAKEN",
                "entity_id": title, 
                "signal_type": f"SEC Form {form_type}",
                "raw_data": {
                    "form": form_type,
                    "title": title,
                    "link": link,
                    "filing_date": updated
                }
            })
            
    if bq_payload:
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=True, # Will automatically create the kraken_edgar table on first run
            )
            job = client.load_table_from_json(bq_payload, table_id, job_config=job_config)
            job.result()  
            print(f"[KRAKEN] Successfully loaded {len(bq_payload)} material filings into BigQuery.")
        except Exception as e:
            print(f"[KRAKEN ERROR] BigQuery push failed: {e}")
    else:
        print("[KRAKEN] No target forms (8-K, 10-K, 10-Q, 4) found in the current live batch.")

if __name__ == "__main__":
    main()
  
