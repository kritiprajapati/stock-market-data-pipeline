from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import json
from azure.storage.blob import BlobServiceClient
import time

# ── Default arguments ───────────────────────────────────────────────────────
default_args = {
    'owner': 'kriti',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

STOCKS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']

# ── Task 1: Fetch stock data from Polygon.io ────────────────────────────────
def fetch_stock_data(**context):
    api_key = Variable.get("polygon_api_key")
    
    execution_date = context['execution_date']
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    # # Get yesterday's date
    # yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    all_stock_data = []
    
    for ticker in STOCKS:
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date_str}?adjusted=true&apiKey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            all_stock_data.append(data)
            print(f"✅ Fetched data for {ticker}")
        else:
            print(f"❌ Failed to fetch {ticker}: {response.status_code}")
    
    context['ti'].xcom_push(key='stock_data', value=all_stock_data)
    context['ti'].xcom_push(key='fetch_date', value=date_str)
    print(f"Total records fetched: {len(all_stock_data)}")

# ── Task 2: Save raw data to ADLS Bronze ────────────────────────────────────
def save_to_bronze(**context):
    stock_data = context['ti'].xcom_pull(key='stock_data', task_ids='fetch_stock_data')
    fetch_date = context['ti'].xcom_pull(key='fetch_date', task_ids='fetch_stock_data')
    
    if not stock_data:
        print("⚠️ No stock data received - market may have been closed yesterday")
        return
    
    account_name = Variable.get("azure_storage_account")
    account_key = Variable.get("azure_storage_key")
    
    connection_string = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    file_name = f"stock_data/{fetch_date}/raw_stocks.json"
    json_data = json.dumps(stock_data, indent=2)
    
    blob_client = blob_service_client.get_blob_client(
        container='bronze',
        blob=file_name
    )
    blob_client.upload_blob(json_data, overwrite=True)
    
    print(f"✅ Saved {len(stock_data)} records to Bronze: {file_name}")
    
    # Push the date to next tasks
    context['ti'].xcom_push(key='processing_date', value=fetch_date)

# ── Task 3: Trigger Silver transformation notebook on Databricks ───────────
def trigger_silver_notebook(**context):
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='save_to_bronze')
    
    workspace_url = Variable.get("databricks_workspace_url")
    pat_token = Variable.get("databricks_pat_token")
    cluster_id = Variable.get("databricks_cluster_id") 
    
    # Databricks notebook path in workspace
    # notebook_path = "/Workspace/stock-market-data-pipeline/notebooks/silver_transformation"
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/silver_transformation"
    
    # Prepare the request
    url = f"{workspace_url}/api/2.1/jobs/runs/submit"
    
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "run_name": f"silver_transformation_{processing_date}",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "execution_date": processing_date
            }
        }
    }
    
    # Submit the job
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger Silver notebook: {response.text}")
    
    run_id = response.json()['run_id']
    print(f"✅ Silver notebook triggered with run_id: {run_id}")
    
    # Wait for job to complete
    wait_for_job_completion(workspace_url, pat_token, run_id, timeout=7200)
    
    context['ti'].xcom_push(key='processing_date', value=processing_date)

# ── Task 4: Trigger Gold transformation notebook on Databricks ─────────────
def trigger_gold_notebook(**context):
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='trigger_silver')
    
    workspace_url = Variable.get("databricks_workspace_url")
    pat_token = Variable.get("databricks_pat_token")
    cluster_id = Variable.get("databricks_cluster_id") 
    
    # notebook_path = "/Workspace/stock-market-data-pipeline/notebooks/gold_transformation"
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/gold_transformation"
    
    url = f"{workspace_url}/api/2.1/jobs/runs/submit"
    
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "run_name": f"gold_transformation_{processing_date}",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "execution_date": processing_date
            }
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger Gold notebook: {response.text}")
    
    run_id = response.json()['run_id']
    print(f"✅ Gold notebook triggered with run_id: {run_id}")
    
    wait_for_job_completion(workspace_url, pat_token, run_id, timeout=7200)
    
    context['ti'].xcom_push(key='processing_date', value=processing_date)

# ── Task 5: Trigger Anomaly detection notebook on Databricks ──────────────
def trigger_anomaly_notebook(**context):
    processing_date = context['ti'].xcom_pull(key='processing_date', task_ids='trigger_gold')
    
    workspace_url = Variable.get("databricks_workspace_url")
    pat_token = Variable.get("databricks_pat_token")
    cluster_id = Variable.get("databricks_cluster_id") 
    
    # notebook_path = "/Workspace/stock-market-data-pipeline/notebooks/anomaly_detection"
    notebook_path = "/Workspace/Users/kritiprajapati140@gmail.com/stock-market-data-pipeline/notebooks/anomaly_detection"
    
    url = f"{workspace_url}/api/2.1/jobs/runs/submit"
    
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "run_name": f"anomaly_detection_{processing_date}",
        "existing_cluster_id": cluster_id,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "execution_date": processing_date
            }
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 200:
        raise AirflowException(f"Failed to trigger Anomaly notebook: {response.text}")
    
    run_id = response.json()['run_id']
    print(f"✅ Anomaly notebook triggered with run_id: {run_id}")
    
    wait_for_job_completion(workspace_url, pat_token, run_id, timeout=7200)

# ── Helper function to wait for job completion ──────────────────────────────
def wait_for_job_completion(workspace_url, pat_token, run_id, timeout=7200):
    """
    Wait for a Databricks job to complete
    timeout: maximum seconds to wait (default 1 hour)
    """
    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Content-Type": "application/json"
    }
    
    url = f"{workspace_url}/api/2.1/jobs/runs/get"
    start_time = time.time()
    
    while True:
        if time.time() - start_time > timeout:
            raise AirflowException(f"Job {run_id} did not complete within {timeout} seconds")
        
        # Get job status
        response = requests.get(url, headers=headers, params={"run_id": run_id})
        
        if response.status_code != 200:
            raise AirflowException(f"Failed to get job status: {response.text}")
        
        run_data = response.json()
        
        life_cycle_state = run_data.get('state', {}).get('life_cycle_state', '')
        result_state = run_data.get('state', {}).get('result_state', '')
        
        print(f"Job {run_id} status: {life_cycle_state} / {result_state}")
        
        if life_cycle_state == 'TERMINATED':
            if result_state == 'SUCCESS':
                print(f"✅ Job {run_id} completed successfully!")
                break
            else:
                raise AirflowException(f"Job {run_id} failed with result: {result_state}")
        elif life_cycle_state == 'INTERNAL_ERROR':
            raise AirflowException(f"Job {run_id} failed with internal error")
        
        # Wait 10 seconds before checking again
        time.sleep(10)

# ── DAG Definition ──────────────────────────────────────────────────────────
with DAG(
    dag_id='stock_market_pipeline',
    default_args=default_args,
    description='Stock market data pipeline with Medallion architecture',
    # schedule='30 3 * * 2-6',  # 9AM IST Tuesday-Saturday
    schedule= None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['stock', 'finance', 'end-to-end']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
    )

    bronze_task = PythonOperator(
        task_id='save_to_bronze',
        python_callable=save_to_bronze,
    )
    
    silver_task = PythonOperator(
        task_id='trigger_silver',
        python_callable=trigger_silver_notebook,
    )
    
    gold_task = PythonOperator(
        task_id='trigger_gold',
        python_callable=trigger_gold_notebook,
    )
    
    anomaly_task = PythonOperator(
        task_id='trigger_anomaly',
        python_callable=trigger_anomaly_notebook,
    )

    # Define task order
    fetch_task >> bronze_task >> silver_task >> gold_task >> anomaly_task