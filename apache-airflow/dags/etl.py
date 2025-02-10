from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import snowflake.connector
from datetime import datetime
import logging
import json
import os
from decimal import Decimal
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Runs every 1 minut
    catchup=False,  # Allows catching up missed runs
    max_active_runs=1,
    is_paused_upon_creation=False
)


def convert_to_json_serializable(value):
    """
    Converts `Decimal` objects to `float`,
    datetime` objects to their ISO string representation,
    and traverses nested structures (lists, tuples, dicts).churn
    """
    if isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, datetime):
        return value.isoformat()  # o str(value) si prefieres
    elif isinstance(value, dict):
        return {k: convert_to_json_serializable(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [convert_to_json_serializable(item) for item in value]
    else:
        return value
    
# ---------------- Get Last Processed Timestamp ---------------- #

def get_last_timestamp(**kwargs):
    """
    Retrieves the last processed timestamp from XCom.
    If not available, defaults to '2000-01-01 00:00:00'.
    """
    ti = kwargs['ti']
    last_timestamp = ti.xcom_pull(task_ids='save_last_timestamp', default="2000-01-01 00:00:00")
    return last_timestamp

# ---------------- Functional Transformation Functions ---------------- #

def transform_online_row(row):
    """
    Transforms a row from the online store into a unified transaction dictionary.
    """
    return {
        "order_type": "online",
        "customer_id": row[1],
        "item_purchased": row[2],
        "category": row[3],
        "purchase_amount_usd": row[4],
        "location": row[5], 
        "size": row[6],
        "color": row[7],
        "season": row[8],
        "review_rating": row[9],
        "shipping_type": row[10],
        "discount_applied": row[11],
        "promo_code_used": row[12],
        "payment_method": row[13],
        "created_at": row[14],
        "updated_at": row[15],  
    }

def transform_physical_row(row):
    """
    Transforms a row from the physical store into a unified transaction dictionary.
    """
    return {
        "order_type": "physical",
        "customer_id": None,
        "item_purchased": row[1],
        "category": row[2],
        "purchase_amount_usd": row[3],
        "location": row[4], 
        "size": row[5],
        "color": row[6],
        "season": row[7],
        "review_rating": None,
        "shipping_type": 'Store Pickup',
        "discount_applied": row[9],
        "promo_code_used": row[9],
        "payment_method": row[10],
        "created_at": row[11],
        "updated_at": row[12],
    }

def transform_customer_row(row):
    """
        Transforms a customer row from the online store into a transaction dictionary.
    """
    return {
        "customer_id":row[0],
        "age":row[1],
        "gender":row[2],
        "previous_purchases":row[3],
        "created_at":row[4],
        "updated_at":row[5],
    }


# ---------------- Extraction Functions ---------------- #

def extract_online_store_data(**kwargs):
    """
    Extracts new data from the online store based on last processed timestamp.
    """
    try:
        ti = kwargs['ti']
        last_timestamp = ti.xcom_pull(task_ids='get_last_timestamp') or "2000-01-01 00:00:00"

        online_hook = PostgresHook(postgres_conn_id='online_store_conn')
        order_sql = f"""
            SELECT * FROM online_orders 
            WHERE created_at > '{last_timestamp}'
            ORDER BY created_at ASC;
        """
        orders_records = online_hook.get_records(order_sql)

        if not orders_records:
            logger.info("No new data from orders online store.")
            return None
        
        orders_records = convert_to_json_serializable(orders_records)  

        orders_file_path = "/tmp/online_oders_data.json"
        
        with open(orders_file_path, "w") as f:
            json.dump(orders_records, f)

        logger.info(f"Extracted orders {len(orders_records)} new rows from db_online_store.")

        customer_sql = f"""
            SELECT * FROM customers 
            WHERE updated_at > '{last_timestamp}'
            ORDER BY created_at ASC;
        """
        customer_records = online_hook.get_records(customer_sql)

        if not customer_records:
            logger.info("No new data from customers online store.")
            return None

        customer_records = convert_to_json_serializable(customer_records)  

        customers_file_path = "/tmp/online_customer_data.json"

        with open(customers_file_path, "w") as f:
            json.dump(customer_records, f)

        logger.info(f"Extracted customers {len(customer_records)} new rows from db_online_store.")

        return orders_file_path,customers_file_path
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise 

def extract_physical_store_data(**kwargs):
    """
    Extracts new data from the physical store based on last processed timestamp.
    """
    try:
        ti = kwargs['ti']
        last_timestamp = ti.xcom_pull(task_ids='get_last_timestamp') or "2000-01-01 00:00:00"

        physical_hook = PostgresHook(postgres_conn_id='physical_store_conn')
        sql = f"""
            SELECT * FROM physical_orders 
            WHERE created_at > '{last_timestamp}'
            ORDER BY created_at ASC;
        """
        records = physical_hook.get_records(sql)

        if not records:
            logger.info("No new data from physical store.")
            return None
        
        records = convert_to_json_serializable(records)  

        file_path = "/tmp/physical_data.json"
        with open(file_path, "w") as f:
            json.dump(records, f)

        logger.info(f"Extracted {len(records)} new rows from db_physical_store.")
        return file_path
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise 

# ---------------- Transformation and Load Function ---------------- #

def transform_and_load(**kwargs):
    """
    Loads extracted data, applies transformations, and inserts into the Data Warehouse.
    """
    try:
        ti = kwargs['ti']
        
        online_orders_file,online_customers_file = ti.xcom_pull(task_ids='extract_online_store_data')
        physical_file = ti.xcom_pull(task_ids='extract_physical_store_data')

        online_orders_data = json.load(open(online_orders_file)) if online_orders_file and os.path.exists(online_orders_file) else []
        online_customers_data = json.load(open(online_customers_file)) if online_customers_file and os.path.exists(online_customers_file) else []

        physical_data = json.load(open(physical_file)) if physical_file and os.path.exists(physical_file) else []


        if not online_orders_data and not physical_data and not online_customers_data:
            logger.info("No new transactions to process.")
            return None

        # Apply transformations
        unified_transactions = [transform_online_row(row) for row in online_orders_data] + \
                            [transform_physical_row(row) for row in physical_data]
        with open('/tmp/prrueba.json', "w") as f:
            json.dump(unified_transactions, f)

        logger.info(f"Unified transactions count: {len(unified_transactions)}")

        
        customers_transactions = [transform_customer_row(row) for row in online_customers_data]

        logger.info(f"Custumers transactions count: {len(unified_transactions)}")



        # Load into Data Warehouse
        dw_hook = snowflake.connector.connect(
            user='dbt_USER',
            password='adminadmin',
            account='yj85292.europe-west2.gcp',
            warehouse='store_data_warehouse',
            database='STOREDATABASE',
            schema='STOREDATA',
        )

        cursor = dw_hook.cursor()

        try:


            customers_columns = list(customers_transactions[0].keys())  

            customers_columns_str = ", ".join(customers_columns)   
            customers_values = ", ".join(["%s"] * len(customers_columns))                 

            insert_customers_sql = f"INSERT INTO CUSTOMERS ({customers_columns_str}) VALUES ({customers_values})"

            all_values = [tuple(tx[col] for col in customers_columns) for tx in customers_transactions]

            cursor.executemany(insert_customers_sql, all_values)
            dw_hook.commit() 
    
            orders_columns = list(unified_transactions[0].keys())  
            
            orders_columns_str = ", ".join(orders_columns)                       
            oders_values = ", ".join(["%s"] * len(orders_columns))        
            insert_orders_sql = f"INSERT INTO ORDERS ({orders_columns_str}) VALUES ({oders_values})"
            


            all_values = [tuple(tx[col] for col in orders_columns) for tx in unified_transactions]
            with open("/tmp/sql.txt", "w", encoding="utf-8") as f:
                f.write(f"INSERT INTO ORDERS ({orders_columns_str}) VALUES ({oders_values}) ({all_values})")
            cursor.executemany(insert_orders_sql, all_values)
            dw_hook.commit() 





        finally:
            cursor.close()
            dw_hook.close()

         # Save last processed timestamp
        last_timestamp = max(row[-2] for row in online_orders_data + physical_data)
        ti.xcom_push(key='last_timestamp', value=last_timestamp)
        logger.info(f"Saved last processed timestamp: {last_timestamp}")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise 
# ---------------- Save Last Processed Timestamp ---------------- #

def save_last_timestamp(**kwargs):
    """
    Saves the last processed timestamp in XCom for future executions.
    """
    ti = kwargs['ti']
    last_timestamp = ti.xcom_pull(task_ids='transform_and_load', key='last_timestamp')
    return last_timestamp

# ---------------- Defining DAG Tasks ---------------- #

# Task to retrieve last processed timestamp
t0 = PythonOperator(
    task_id='get_last_timestamp',
    python_callable=get_last_timestamp,
    dag=dag,
)

# Task to extract online data
t1 = PythonOperator(
    task_id='extract_online_store_data',
    python_callable=extract_online_store_data,
    dag=dag,
)

# Task to extract physical store data
t2 = PythonOperator(
    task_id='extract_physical_store_data',
    python_callable=extract_physical_store_data,
    dag=dag,
)

# Task to transform and load data into the Data Warehouse
t3 = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_and_load,
    dag=dag,
)

# Task to save the last processed timestamp
t4 = PythonOperator(
    task_id='save_last_timestamp',
    python_callable=save_last_timestamp,
    dag=dag,
)

# Establish task dependencies
t0 >> [t1, t2] >> t3 >> t4
