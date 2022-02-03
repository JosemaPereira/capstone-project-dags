import airflow.utils.dates
import numpy as np
from airflow import DAG

from custom_modules.s3_to_postgres import S3ToPostgresTransfer

USER = 'josema.pereira'
TASK_PRODUCTS_ID = 'dag_s3_to_postgres_products'
TASK_DAG_NAME = 'dag_insert_data_products'
SCHEMA = 'bootcampdb'
PRODUCTS_TABLE = 'user_purchase'
S3_BUCKET = 's3-data-bootcamp-20220104171236432500000005'
S3_PRODUCTS_FILE = 'user_purchase.csv'
AWS_CONNECTION_ID = 'aws_default'
POSTGRESS_PRODUCT_CONNECTION = 'default_postgres'

default_args = {
    "owner": USER,
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
}

schema = {
    "InvoiceNo": "str",
    "StockCode": "str",
    "Description": "str",
    "Quantity": np.float64,
    "UnitPrice": np.float64,
    "CustomerID": np.float64,
    "Country": "str",
}
query_file_name = "bootcampdb.user_purchase.sql"
list_target_fields = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]
dag = DAG(TASK_DAG_NAME, default_args=default_args, schedule_interval="@daily")

process_dag = S3ToPostgresTransfer(
    task_id=TASK_PRODUCTS_ID,
    schema=SCHEMA,
    table=PRODUCTS_TABLE,
    table_schema=schema,
    query_file_name=query_file_name,
    list_target_fields=list_target_fields,
    s3_bucket=S3_BUCKET,
    s3_key=S3_PRODUCTS_FILE,
    aws_conn_postgres_id=POSTGRESS_PRODUCT_CONNECTION,
    aws_conn_id=AWS_CONNECTION_ID,
    dag=dag,
)

process_dag