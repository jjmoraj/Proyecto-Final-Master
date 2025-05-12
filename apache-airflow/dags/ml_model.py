from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import pickle
import json
import logging
import os
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'holt_winters_forecasting',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Runs daily at midnight UTC
    catchup=False,
    max_active_runs=1,
    tags=['forecasting'],
)

# ---------------- Utility Function ---------------- #

def convert_to_json_serializable(value):
    """
    Converts numpy types and datetime objects into JSON-serializable Python primitives.
    """
    if isinstance(value, np.generic):
        return value.item()
    elif isinstance(value, (list, tuple, np.ndarray)):
        return [convert_to_json_serializable(v) for v in value]
    elif isinstance(value, datetime):
        return value.isoformat()
    else:
        return value

# ---------------- Extraction Functions ---------------- #

def extract_daily_counts(**kwargs):
    """
    Extracts daily order counts from Snowflake and writes them to JSON.
    """
    try:
        ti = kwargs['ti']
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
        )
        cursor = conn.cursor()
        query = """
            SELECT
                DATE(CREATED_AT) AS ds,
                COUNT(*)           AS y
            FROM ORDERS
            GROUP BY DATE(CREATED_AT)
            ORDER BY ds
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        # Build DataFrame
        df = pd.DataFrame(rows, columns=['ds', 'y'])
        df['ds'] = pd.to_datetime(df['ds'])

        # Serialize to JSON-friendly dict
        data = df.to_dict(orient='list')
        data = {k: convert_to_json_serializable(v) for k, v in data.items()}

        # Write to disk
        out_path = '/tmp/daily_counts.json'
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        logger.info(f"Extracted {len(df)} daily counts → {out_path}")

        # Push path for downstream tasks
        ti.xcom_push(key='daily_counts_path', value=out_path)

    except Exception as e:
        logger.error(f"Error in extract_daily_counts: {e}")
        raise

# ---------------- Forecasting Functions ---------------- #

def run_holt_winters(**kwargs):
    """
    Loads daily counts JSON, performs Holt-Winters validation and forecasting,
    saves evaluation metrics, trained model, and 7-day forecast.
    """
    try:
        ti   = kwargs['ti']
        # recupera el path al JSON con {'ds':'YYYY-MM-DD','y':int} desde la tarea previa
        json_path = ti.xcom_pull(task_ids='extract_daily_counts', key='daily_counts_path')

        # 1) Cargo y preparo el DataFrame
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df['ds'] = pd.to_datetime(df['ds'])
        df.set_index('ds', inplace=True)
        df = df.sort_index().asfreq('D', fill_value=0)

        # 2) Entreno el modelo sobre TODO el histórico
        model_full = ExponentialSmoothing(
            df['y'].astype(float),
            trend='add',
            seasonal='add',
            seasonal_periods=7
        ).fit(optimized=True)

        # 3) Serializo el modelo + última fecha en un solo archivo
        last_date  = df.index.max()
        artifact   = {
            'model': model_full,
            'last_date': last_date
        }
        out_dir = '/opt/airflow/data/model'
        os.makedirs(out_dir, exist_ok=True)
        model_path = os.path.join(out_dir, 'holt_winters_artifact.pkl')

        with open(model_path, 'wb') as f:
            pickle.dump(artifact, f)

        logger.info(f"Modelo y metadata guardados en {model_path}")


    except Exception as e:
        logger.error(f"Error in run_holt_winters: {e}")
        raise

# ---------------- Defining DAG Tasks ---------------- #

t1 = PythonOperator(
    task_id='extract_daily_counts',
    python_callable=extract_daily_counts,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_holt_winters',
    python_callable=run_holt_winters,
    provide_context=True,
    dag=dag,
)

# Task dependencies
t1 >> t2
