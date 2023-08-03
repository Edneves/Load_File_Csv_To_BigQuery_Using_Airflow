from global_modules.functions import getLocalConfig
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.empty import EmptyOperator
import json
from datetime import (datetime, timedelta)


DAG_ID = 'LOAD_FILE_CSV_TO_BIGQUERY'

yaml_data = getLocalConfig(DAG_ID)
TEMPLATE_SEARCH_PATH = f"{yaml_data['info_params']['template_search']['search_path']}{DAG_ID}"
TEMPLATE_SEARCH_PATH_GLOBAL = yaml_data['info_params']['template_search']['search_path_global']

# INFORMATION ENVIRONMENT CLOUD GCP
project_id = yaml_data['info_params']['info_tables']['project_id']
dataset_rz = yaml_data['info_params']['info_tables']['dataset_rz']
table_id= yaml_data['info_params']['info_tables']['table_id']

# INFORMATION FILE IN BUCKET
file = yaml_data['info_params']['file']['name']
bucket = yaml_data['info_params']['path']['bucket']
input = yaml_data['info_params']['path']['input']
processing = yaml_data['info_params']['path']['processing']
output = yaml_data['info_params']['path']['output']

source_object_IN = f'{input}/{file}.csv'
source_object_PR = f'{processing}/{file}.csv'

with open(f'{TEMPLATE_SEARCH_PATH}/schema/{file}.json') as f:
    schema = json.load(f)

default_args = {
    'owner': 'Ednaldo',
    'start_date': datetime(2023, 8, 2),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 2
}

with DAG(
    DAG_ID,
    description='Ingestão de arquivo csv para o BigQuery, camada raz_zone',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=['raw_zone', 'ingestion'],
    template_searchpath=[TEMPLATE_SEARCH_PATH, TEMPLATE_SEARCH_PATH_GLOBAL]
) as dag:

    check_existence = GCSObjectExistenceSensor(
        task_id=f'Validate_file_{file}.csv',
        bucket=bucket,
        object=source_object_IN,
        timeout=180
    )
    begin_task = EmptyOperator(
        task_id=f'Begin_{DAG_ID}'
    )
    moving_file = GCSToGCSOperator(
        task_id=f'Moving_file_{file}.csv_path_processing',
        source_bucket=bucket,
        source_object=source_object_IN,
        destination_bucket=bucket,
        destination_object=f'{processing}/'
    )
    load_to_bigquery = GCSToBigQueryOperator(
        task_id=f'Load_data_{file}.csv_to_bigquery',
        bucket=bucket,
        source_objects=source_object_PR,
        destination_project_dataset_table=f'{project_id}.{dataset_rz}.{table_id}',
        schema_fields=schema,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        ignore_unknown_values=True,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        field_delimiter=','
    )
    moving_output = GCSToGCSOperator(
        task_id=f'Moving_file_{file}.csv_path_output',
        source_bucket=bucket,
        source_object=source_object_PR,
        destination_bucket=bucket,
        destination_object=f'{output}/'
    )
    end_task = EmptyOperator(
        task_id=f'End_{DAG_ID}'
    )

    check_existence >> begin_task >> moving_file >> load_to_bigquery >> moving_output >> end_task

















