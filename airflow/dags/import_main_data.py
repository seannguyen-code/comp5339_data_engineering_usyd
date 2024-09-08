from datetime import timedelta, datetime
import pytz
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

# Fetching connection strings from environment variables
AIRFLOW_CONN_SALES_DW = os.getenv('AIRFLOW_CONN_SALES_DW')
AIRFLOW_CONN_SALES_OLTP = os.getenv('AIRFLOW_CONN_SALES_OLTP')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Don't wait for previous runs to complete
    'email': ['airflow@example.com'],  # Email for notifications
    'email_on_failure': False,  # Disable email on failure
    'email_on_retry': False,  # Disable email on retry
    'retries': 0,  # Number of retries
    'retry_delay': timedelta(minutes=5)  # Delay between retries
}

# Define the DAG
dag = DAG(
    'import_main_data',
    default_args=default_args,
    description='Import Main Transactions Files',  # Description of the DAG
    schedule_interval='@daily',  # Schedule interval
    start_date=days_ago(1),  # Start date
    is_paused_upon_creation=False  # The DAG is not paused upon creation
)

# Sensor to wait for the initialization of the ETL environment
wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',  # External DAG to wait for
    execution_date_fn = lambda x: datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),  # Execution date function
    timeout=1,  # Timeout for the sensor
    dag=dag
)

# Task to import transactions data
import_transactions_task = BashOperator(
    task_id='import_transactions',
    bash_command=f"""psql {AIRFLOW_CONN_SALES_OLTP} -c "\copy transactions to stdout" | psql {AIRFLOW_CONN_SALES_DW} -c "\copy import.transactions(transaction_id, customer_id, product_id, amount, qty, channel_id, bought_date)  from stdin" """,
    dag=dag,
)

# Task to import channels data
import_channels_task = BashOperator()

# Task to import resellers data
import_resellers_task = BashOperator()

# Task to import customers data
import_customers_task = BashOperator()

# Task to import products data
import_products_task = BashOperator()


# Define the task dependencies
# wait_for_init >> import_transactions_task >> []
