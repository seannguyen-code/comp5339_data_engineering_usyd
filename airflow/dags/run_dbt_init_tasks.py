# Import necessary libraries and modules
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    "start_date": days_ago(1),  # Start date for the DAG
    "retries": 1,  # Number of retries
    "retry_delay": timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
with DAG(dag_id='run_dbt_init_tasks', default_args=default_args, schedule_interval='@once', ) as dag:

    # Task to wait for the completion of 'import_main_data' DAG
    wait_for_import_main_data = ExternalTaskSensor(
        task_id='wait_for_import_main_data',
        external_dag_id='import_main_data',
        external_task_id=None,  # If you want to wait for a specific task, set it here; otherwise, waits for the whole DAG
        timeout=300,  # Timeout after 5 minutes
        mode='poke',
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # Task to wait for the completion of 'import_reseller_data' DAG
    wait_for_import_reseller_data = ExternalTaskSensor(
        task_id='wait_for_import_reseller_data',
        external_dag_id='import_reseller_data',
        external_task_id=None,  # If you want to wait for a specific task, set it here
        timeout=300,  # Timeout after 5 minutes
        mode='poke',
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # Task to pull the most recent version of the dependencies listed in packages.yml from git
    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /usr/local/airflow/dbt && dbt deps",
    )

    # Task to seed the database with data defined in dbt seed files
    dbt_seed_task = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /usr/local/airflow/dbt && dbt seed --target dev",
    )

    # Define task dependencies
    wait_for_import_main_data >> wait_for_import_reseller_data >> dbt_deps_task >> dbt_seed_task
