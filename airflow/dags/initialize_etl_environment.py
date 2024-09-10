from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'initialize_etl_environment',
    default_args=default_args,
    description='Initialize ETL Environment',
    schedule_interval='@once',  # Run the DAG once
    start_date=datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),  # Start date of the DAG
    tags=['init'],
    is_paused_upon_creation=False
)

# Task to create necessary schemas in the database
create_schemas_task = PostgresOperator(
    task_id='create_schemas',
    sql="""
    CREATE SCHEMA IF NOT EXISTS import;
    CREATE SCHEMA IF NOT EXISTS warehouse;
    CREATE SCHEMA IF NOT EXISTS ops;
    CREATE SCHEMA IF NOT EXISTS staging;
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a registry table for tracking file loads
create_registry_table = PostgresOperator(
    task_id='create_registry_table',
    sql="""
    CREATE TABLE IF NOT EXISTS ops.FlatFileLoadRegistry (
        Filename VARCHAR(255) PRIMARY KEY,
        Extension VARCHAR(10),
        LoadDate TIMESTAMP,
        Processed BOOLEAN,
        Validated BOOLEAN
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for CSV imports
create_csv_destination = PostgresOperator(
    task_id='create_csv_destination',
    sql="""
    CREATE TABLE IF NOT EXISTS import.ResellerCSV (
        reseller_id VARCHAR(255),  -- Allow for alphanumeric IDs
        transaction_id VARCHAR(255),
        product_name VARCHAR(255),
        total_amount NUMERIC,
        number_of_purchased_postcards INT,
        created_date DATE,
        office_location VARCHAR(255),
        sales_channel VARCHAR(255)
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for XML imports
create_xml_destination_task = PostgresOperator(
    task_id='create_xml_destination',
    sql="""
    CREATE TABLE IF NOT EXISTS import.ResellerXML (
        reseller_id VARCHAR(255),  -- Allow for alphanumeric IDs
        transaction_id VARCHAR(255),
        product_name VARCHAR(255),
        total_amount NUMERIC,
        no_purchased_postcards INT,
        date_bought DATE,
        office_location VARCHAR(255),
        sales_channel VARCHAR(255)
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for transaction data
create_transactions_task = PostgresOperator(
    task_id='create_transactions',
    sql="""
    CREATE TABLE IF NOT EXISTS import.transactions (
        transaction_id VARCHAR(255),
        customer_id VARCHAR(255),  -- Adjusted to handle alphanumeric IDs
        product_id VARCHAR(255),   -- Allow for alphanumeric product IDs
        amount NUMERIC,
        qty INT,
        channel_id VARCHAR(255),   -- Adjusted to match channel_id datatype
        bought_date DATE
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for reseller data
create_resellers_task = PostgresOperator(
    task_id='create_resellers',
    sql="""
    CREATE TABLE IF NOT EXISTS import.resellers (
        reseller_id VARCHAR(255),  -- Allow for alphanumeric IDs
        reseller_name VARCHAR(255),
        commission_pct NUMERIC
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for channel data
create_channels_task = PostgresOperator(
    task_id='create_channels',
    sql="""
    CREATE TABLE IF NOT EXISTS import.channels (
        channel_id VARCHAR(255),   -- Adjusted to handle alphanumeric IDs
        channel_name VARCHAR(255)
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for customer data
create_customers_task = PostgresOperator(
    task_id='create_customers',
    sql="""
    CREATE TABLE IF NOT EXISTS import.customers (
        customer_id VARCHAR(255),  -- Adjusted to handle alphanumeric IDs
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        email VARCHAR(255)
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Task to create a destination table for product data
create_products_task = PostgresOperator(
    task_id='create_products',
    sql="""
    CREATE TABLE IF NOT EXISTS import.products (
        product_id VARCHAR(255),   -- Adjusted to handle alphanumeric product IDs
        name VARCHAR(255),
        price NUMERIC
    );
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Define the task dependencies
create_schemas_task >> create_registry_table
create_schemas_task >> [create_csv_destination, create_xml_destination_task, create_transactions_task, create_resellers_task, create_channels_task, create_customers_task, create_products_task]
