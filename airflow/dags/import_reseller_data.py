from datetime import timedelta, datetime
import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import os
import pandas as pd
import json
import xmltodict


# Clean up processed files on DAG initialization
try:
    for directory_path in ['/import/csv/processed/', '/import/xml/processed/']:
        # Get a list of all files in the directory
        files = os.listdir(directory_path)

        # Remove each file in the directory
        for file in files:
            file_path = os.path.join(directory_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
except:
    pass

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Don't wait for previous runs to complete
    'email': ['airflow@example.com'],  # Email for notifications
    'email_on_failure': False,  # Disable email on failure
    'email_on_retry': False,  # Disable email on retry
    'retries': 0,  # Number of retries
    'retry_delay': timedelta(minutes=1)  # Delay between retries
}

########################################################################################################################
# Read and complete the code files for the DAG
# Define the DAG
dag = DAG(
    'import_reseller_data',
    default_args=default_args,
    description='Import Resellers Transactions Files',  # Description of the DAG
    schedule_interval='@daily',  # Schedule interval
    start_date=days_ago(1),  # Start date
    tags=['csv', 'reseller'],  # Tags for categorizing the DAG
    is_paused_upon_creation=False  # The DAG is not paused upon creation
)

# Establish a connection to the Postgres database
POSTGRES_HOOK = PostgresHook('sales_dw')
ENGINE = POSTGRES_HOOK.get_sqlalchemy_engine()

# Sensor to wait for the initialization of the ETL environment
wait_for_init = ExternalTaskSensor(
    task_id='wait_for_init',
    external_dag_id='initialize_etl_environment',  # External DAG to wait for
    execution_date_fn=lambda x: datetime(2024, 1, 1, 0, 0, 0, 0, pytz.UTC),  # Execution date function
    timeout=1,  # Timeout for the sensor
    dag=dag
)


# Function to get validated files from the registry
def get_validated(filetype):
    with ENGINE.connect() as con:
        result = con.execute(
            f"""SELECT Filename FROM ops.FlatFileLoadRegistry where validated=True and extension='{filetype}' """)
        return set(row.values()[0] for row in result)


# Function to get processed files from the registry
def get_processed(filetype):
    with ENGINE.connect() as con:
        result = con.execute(
            f"""SELECT Filename FROM ops.FlatFileLoadRegistry where processed=True and extension='{filetype}' """)
        return set(row.values()[0] for row in result)


# Function to update the flatfile registry
def update_flatfile_registry(file_data):
    command = f"""
    INSERT INTO ops.FlatFileLoadRegistry(Filename, Extension, LoadDate, Processed, Validated)
    VALUES('{file_data['filename']}','{file_data['extension']}','{file_data['loaddate']}',{file_data['processed']}, {file_data['validated']} ) 
    ON CONFLICT (Filename) 
    DO UPDATE SET processed={file_data['processed']}, validated={file_data['validated']}, loaddate='{file_data['loaddate']}';
    """
    with ENGINE.connect() as con:
        con.execute(command)


# Function to preprocess CSV files
def preprocess_csv():
    IMPORT_PATH = '/import/csv/raw/'
    EXPORT_PATH = '/import/csv/processed/'
    PROCESSED = get_processed('csv')
    IGNORED = '.keep'

    for file in sorted(os.listdir(IMPORT_PATH)):
        if file not in PROCESSED and file != IGNORED:
            extension = file.split('.')[-1]
            df = pd.read_csv(IMPORT_PATH + file, encoding='utf-8')
            df['Imported_File'] = file
            df.to_csv(EXPORT_PATH + file, index=False)

            file_data = {'filename': file, 'extension': extension, 'loaddate': datetime.now(), 'processed': True,
                         'validated': False}
            update_flatfile_registry(file_data)

            print(f'Processed {file}')


# Function to import CSV files into the database
def import_csv():
    PATH = '/import/csv/processed'
    VALIDATED = get_validated('csv')
    IGNORED = '.keep'

    for file in sorted(os.listdir(PATH)):
        if file not in VALIDATED and file != IGNORED:
            extension = file.split('.')[-1]
            SQL_STATEMENT = """
            COPY import.ResellerCSV() FROM STDIN DELIMITER ',' CSV HEADER;
            """
            conn = POSTGRES_HOOK.get_conn()
            cur = conn.cursor()

            try:
                with open(os.path.join(PATH, file), mode='r', encoding='utf-8', errors='replace') as f:
                    cur.copy_expert(SQL_STATEMENT, f)
                conn.commit()

                file_data = {
                    'filename': file,
                    'extension': extension,
                    'loaddate': datetime.now(),
                    'processed': True,
                    'validated': True
                }
                update_flatfile_registry(file_data)
                print(f'Imported {file}')
            except UnicodeDecodeError as e:
                print(f"Unicode decode error in file {file}: {e}")
            except Exception as e:
                print(f"Unexpected error with file {file}: {e}")
            finally:
                cur.close()
                conn.close()


# Function to preprocess XML files
def preprocess_xml():
    IMPORT_PATH = '/import/xml/raw/'
    EXPORT_PATH = '/import/xml/processed/'
    IGNORED = '.keep'

    PROCESSED = get_processed('xml')

    for file in sorted(os.listdir(IMPORT_PATH)):
        if file not in PROCESSED and file != IGNORED:
            with open(IMPORT_PATH + file, 'r') as myfile:
                obj = xmltodict.parse(myfile.read())

            if obj['transactions']:
                with open(EXPORT_PATH + file[:-4] + '.json', 'w', encoding='utf-8') as f:
                    for e in dict(obj['transactions'])['transaction']:
                        json.dump(e, f, ensure_ascii=False)
                        f.write('\n')

                file_data = {'filename': file, 'extension': 'xml', 'loaddate': datetime.now(), 'processed': True,
                             'validated': False}
            else:
                file_data = {'filename': file, 'extension': 'xml', 'loaddate': datetime.now(), 'processed': False,
                             'validated': False}

            update_flatfile_registry(file_data)

        print(f'Processed {file}')


# Function to import XML files into the database
def import_xml():
    PATH = '/import/xml/processed'
    VALIDATED = get_validated('xml')
    IGNORED = '.keep'

    for file in sorted(os.listdir(PATH)):
        if file not in VALIDATED and file != IGNORED:
            SQL_STATEMENT = """
            COPY import.ResellerXML(data) FROM STDIN;
            """
            conn = POSTGRES_HOOK.get_conn()
            cur = conn.cursor()

            try:
                with open(os.path.join(PATH, file), mode='r', encoding='utf-8', errors='replace') as f:
                    cur.copy_expert(SQL_STATEMENT, f)
                conn.commit()

                filename = file[:-4]
                xml_filename = filename + '.xml'

                file_data = {
                    'filename': xml_filename,
                    'extension': 'xml',
                    'loaddate': datetime.now(),
                    'processed': True,
                    'validated': True
                }
                update_flatfile_registry(file_data)
                print(f'Imported {xml_filename} ({file})')
            except UnicodeDecodeError as e:
                print(f"Unicode decode error in file {file}: {e}")
            except Exception as e:
                print(f"Unexpected error with file {file}: {e}")
            finally:
                cur.close()
                conn.close()


# Define the PythonOperator for CSV preprocessing
preprocess_csv = PythonOperator(
    task_id='preprocess_csv',
    python_callable=preprocess_csv,
    dag=dag
)

# Define the PythonOperator for importing CSV files
import_csv = PythonOperator(
    task_id='import_csv',
    python_callable=import_csv,
    dag=dag
)

# Define the PythonOperator for XML preprocessing
preprocess_xml = PythonOperator(
    task_id='preprocess_xml',
    python_callable=preprocess_xml,
    dag=dag
)

# Define the PythonOperator for importing XML files
import_xml = PythonOperator(
    task_id='import_xml',
    python_callable=import_xml,
    dag=dag
)

# Define the PostgresOperator to create the destination table
create_transform_reseller_destination = PostgresOperator(
    task_id='create_transform_reseller_destination',
    sql="""
        CREATE TABLE IF NOT EXISTS staging.ResellerXmlExtracted (
        reseller_id int,
        )
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Define the PostgresOperator to insert transformed reseller data
insert_transform_reseller = PostgresOperator(
    task_id='insert_transform_reseller',
    sql="""
        INSERT INTO staging.ResellerXmlExtracted (
        reseller_id,
        )
        SELECT 
        
        FROM import.resellerxml
    """,
    dag=dag,
    postgres_conn_id='sales_dw',
    autocommit=True
)

# Define the task dependencies
create_transform_reseller_destination >> ##

wait_for_init >> [##, ##]
preprocess_csv >> ##
preprocess_xml >> ##
#import_xml >> ### >> ###
