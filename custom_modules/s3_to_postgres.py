from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
import numpy as np
import pandas as pd
import io
import os.path


class S3ToPostgresTransfer(BaseOperator):
    template_fields = {}
    template_ext = {}
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        schema,
        table,
        s3_bucket,
        s3_key,
        aws_conn_postgres_id,
        aws_conn_id,
        verify=None,
        wildcard_match=False,
        copy_options=tuple(),
        autocommit=False,
        parameters=None,
        *args,
        **kwargs
    ):
        super(S3ToPostgresTransfer, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_conn_postgres_id = aws_conn_postgres_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.wildcard_match = wildcard_match
        self.copy_options = copy_options
        self.autocommit = autocommit
        self.parameters = parameters

    def execute(self, context):
        self.log.info("Current Postgress Connection " + self.aws_conn_postgres_id)

        self.pg_hook = PostgresHook(postgre_conn_id=self.aws_conn_postgres_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info("Downloading S3 file " + self.s3_key + ", " + self.s3_bucket)

        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key, self.s3_bucket):
                raise AirflowException("No key matches {0}".format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key, self.s3_bucket)
        else:
            if not self.s3.check_for_key(self.s3_key, self.s3_bucket):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key)
                )
            s3_key_object = self.s3.get_key(self.s3_key, self.s3_bucket)

        list_srt_content = (
            s3_key_object.get()["Body"].read().decode(encoding="utf-8", errors="ignore")
        )

        schema = {
            "InvoiceNo": "str",
            "StockCode": "str",
            "Description": "str",
            "Quantity": np.float64,
            "UnitPrice": np.float64,
            "CustomerID": np.float64,
            "Country": "str",
        }

        df_products = pd.read_csv(
            io.StringIO(list_srt_content),
            header=0,
            delimiter=",",
            quotechar='"',
            low_memory=False,
            dtype=schema,
        )

        df_products = df_products.replace(r"[\"]", r"'")
        list_df_products = df_products.values.tolist()
        list_df_products = [tuple(x) for x in list_df_products]

        query_file_name = "bootcampdb.user_purchase.sql"

        query_path = (
            "/opt/airflow/dags/repo/custom_modules/assets" + os.path.sep + query_file_name
        )
        self.log.info("Querry path:" + query_path)
        permissions = "r"
        codification = "UTF-8"
        with open(query_path, permissions, encoding=codification) as file_controller:
            SQL_COMMAND_CREATE_TBL = file_controller.read()
            file_controller.close()
            self.log.info(SQL_COMMAND_CREATE_TBL)
        self.pg_hook.run(SQL_COMMAND_CREATE_TBL)

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

        self.current_table = self.schema + "." + self.table
        self.pg_hook.insert_rows(
            self.current_table,
            list_df_products,
            target_fields=list_target_fields,
            commit_every=1000,
            replace=False,
        )

        self.request = "SELECT * FROM " + self.current_table
        self.log.info(self.request)
        self.connection = self.pg_hook.get_conn()
        self.cursor = self.connection.cursor()
        self.cursor.execute(self.request)
        self.sources = self.cursor.fetchall()
