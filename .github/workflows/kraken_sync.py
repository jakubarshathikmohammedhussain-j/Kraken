name: KRAKEN Execution Node (SEC EDGAR)

on:
  schedule:
    - cron: '0 */4 * * 1-5' # Runs every 4 hours, Monday through Friday
  workflow_dispatch:

jobs:
  execute-kraken-pipeline:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4
        
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'
          
      - name: Install Dependencies
        run: pip install requests google-cloud-bigquery
        
      - name: Execute KRAKEN Stream
        env:
          GOOGLE_CREDENTIALS: ${{ secrets.GOOGLE_CREDENTIALS }}
        run: python kraken_sync.py

