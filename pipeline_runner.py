import requests
import json
import os
import time
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient

# ── Configuration from environment variables ──────────────────────────────────
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")
DATABRICKS_WORKSPACE_URL = os.environ.get("DATABRICKS_WORKSPACE_URL")
DATABRICKS_PAT_TOKEN = os.environ.get("DATABRICKS_PAT_TOKEN")
DATABRICKS_CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID")

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']

# ── Helper: Get last trading day ──────────────────────────────────────────────
def get_last_trading_day():
    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    
    # Skip weekends
    if yesterday.weekday() == 5:  # Saturday
        yesterday = yesterday - timedelta(days=1)
    elif yesterday.weekday() == 6:  # Sunday
        yesterday = yesterday - timedelta(days=2)
    
    return yesterday.strftime('%Y-%m-%d')

# ── Step 1: Fetch stock data from Polygon.io ──────────────────────────────────
def fetch_stock_data(date_str):
    print(f"\n{'='*50}")
    print(f"STEP 1: Fetching stock data for {date_str}")
    print(f"{'='*50}")
    
    all_stock_data = []
    
    for ticker in STOCKS:
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date_str}?adjusted=true&apiKey={POLYGON_API_KEY}"
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            all_stock_data.append(data)
            print(f"✅ Fetched {ticker}: close={data.get('close')}")
        else:
            print(f"❌ Failed {ticker}: {response.status_code} - {response.text}")
    
    print(f"\nTotal records fetched: {len(all_stock_data)}")
    return all_stock_data

# ── Step 2: Save to Bronze ────────────────────────────────────────────────────
def save_to_bronze(stock_data, date_str):
    print(f"\n{'='*50}")
    print(f"STEP 2: Saving to Bronze layer")
    print(f"{'='*50}")
    
    if not stock_data:
        print("⚠️ No data to save - market may have been closed")
        return False
    
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT};"
        f"AccountKey={AZURE_STORAGE_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    file_name = f"stock_data/{date_str}/raw_stocks.json"
    json_data = json.dumps(stock_data, indent=2)
    
    blob_client = blob_service_client.get_blob_client(
        container='bronze',
        blob=file_name
    )
    blob_client.upload_blob(json_data, overwrite=True)
    
    print(f"✅ Saved {len(stock_data)} records to Bronze: {file_name}")
    return True

# ── Helper: Trigger Databricks notebook ──────────────────────────────────────
def trigger_databricks_notebook(notebook_path, run_name, date_str):
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "run_name": run_name,
        "existing_cluster_id": DATABRICKS_CLUSTER_ID,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "execution_date": date_str
            }
        }
    }
    
    url = f"{DATABRICKS_WORKSPACE_URL}/api/2.1/jobs/runs/submit"
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"Failed to trigger notebook: {response.text}")
    
    run_id = response.json()['run_id']
    print(f"✅ Notebook triggered with run_id: {run_id}")
    return run_id

# ── Helper: Wait for Databricks job ──────────────────────────────────────────
def wait_for_job(run_id, timeout=3600):
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PAT_TOKEN}",
        "Content-Type": "application/json"
    }
    
    url = f"{DATABRICKS_WORKSPACE_URL}/api/2.1/jobs/runs/get"
    start_time = time.time()
    
    while True:
        if time.time() - start_time > timeout:
            raise Exception(f"Job {run_id} timed out after {timeout} seconds")
        
        response = requests.get(url, headers=headers, params={"run_id": run_id}, timeout=30)
        run_data = response.json()
        
        life_cycle_state = run_data.get('state', {}).get('life_cycle_state', '')
        result_state = run_data.get('state', {}).get('result_state', '')
        
        print(f"Job {run_id}: {life_cycle_state} / {result_state}")
        
        if life_cycle_state == 'TERMINATED':
            if result_state == 'SUCCESS':
                print(f"✅ Job {run_id} completed successfully!")
                return True
            else:
                raise Exception(f"Job {run_id} failed: {result_state}")
        
        time.sleep(15)

# ── Step 3: Trigger Silver notebook ──────────────────────────────────────────
def run_silver(date_str):
    print(f"\n{'='*50}")
    print(f"STEP 3: Running Silver transformation")
    print(f"{'='*50}")
    
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/silver_transformation"
    run_id = trigger_databricks_notebook(notebook_path, f"silver_{date_str}", date_str)
    wait_for_job(run_id)

# ── Step 4: Trigger Gold notebook ────────────────────────────────────────────
def run_gold(date_str):
    print(f"\n{'='*50}")
    print(f"STEP 4: Running Gold transformation")
    print(f"{'='*50}")
    
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/gold_transformation"
    run_id = trigger_databricks_notebook(notebook_path, f"gold_{date_str}", date_str)
    wait_for_job(run_id)

# ── Step 5: Trigger Anomaly notebook ─────────────────────────────────────────
def run_anomaly(date_str):
    print(f"\n{'='*50}")
    print(f"STEP 5: Running Anomaly Detection")
    print(f"{'='*50}")
    
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/anomaly_detection"
    run_id = trigger_databricks_notebook(notebook_path, f"anomaly_{date_str}", date_str)
    wait_for_job(run_id)

# ── Main: Run full pipeline ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*50)
    print("STOCK MARKET DATA PIPELINE")
    print("="*50)
    
    # Get date
    date_str = get_last_trading_day()
    print(f"Processing date: {date_str}")
    
    # Run all steps
    stock_data = fetch_stock_data(date_str)
    
    if not stock_data:
        print("⚠️ No data fetched - pipeline stopping")
        exit(0)
    
    save_to_bronze(stock_data, date_str)
    run_silver(date_str)
    run_gold(date_str)
    run_anomaly(date_str)
    
    print("\n" + "="*50)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*50)