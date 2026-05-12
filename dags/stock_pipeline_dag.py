from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
from azure.storage.blob import BlobServiceClient

# ── Default arguments ───────────────────────────────────────────────────────
default_args = {
    'owner': 'kriti',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']

# ── Task 1: Fetch stock data ─────────────────────────────────────────────────
def fetch_stock_data(ti, **context):
    api_key = Variable.get("polygon_api_key")
    
    # Get yesterday's date
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    all_stock_data = []
    
    for ticker in STOCKS:
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{yesterday}?adjusted=true&apiKey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            all_stock_data.append(data)
            print(f"✅ Fetched {ticker}: open={data.get('open')}, close={data.get('close')}")
        else:
            print(f"❌ Failed {ticker}: status={response.status_code}, response={response.text}")
    
    print(f"Total records fetched: {len(all_stock_data)}")
    
    # Push to XCom
    ti.xcom_push(key='stock_data', value=all_stock_data)
    ti.xcom_push(key='fetch_date', value=yesterday)

# ── Task 2: Save to Bronze ───────────────────────────────────────────────────
def save_to_bronze(ti, **context):
    # Pull data from previous task
    stock_data = ti.xcom_pull(key='stock_data', task_ids='fetch_stock_data')
    fetch_date = ti.xcom_pull(key='fetch_date', task_ids='fetch_stock_data')
    
    print(f"Received {len(stock_data) if stock_data else 0} records for date {fetch_date}")
    
    if not stock_data:
        print("⚠️ No stock data received - market may have been closed yesterday")
        # Don't raise error - just skip gracefully
        return
    
    # Get Azure credentials
    account_name = Variable.get("azure_storage_account")
    account_key = Variable.get("azure_storage_key")
    
    # Connect to Azure
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # File path with date partitioning
    file_name = f"stock_data/{fetch_date}/raw_stocks.json"
    json_data = json.dumps(stock_data, indent=2)
    
    # Upload to Bronze
    blob_client = blob_service_client.get_blob_client(
        container='bronze',
        blob=file_name
    )
    blob_client.upload_blob(json_data, overwrite=True)
    
    print(f"✅ Saved {len(stock_data)} records to Bronze container: {file_name}")

# ── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id='stock_market_pipeline',
    default_args=default_args,
    description='Stock market data pipeline with Medallion architecture',
    schedule='30 3 * * 2-6',
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['stock', 'finance', 'bronze']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
    )

    bronze_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )

    fetch_task >> bronze_task