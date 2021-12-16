from airflow import DAG
from airflow.operators.bash import BashOperator
import airflow.utils.dates

default_args = {
    "owner": "system",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
}

dag = DAG("dag_install_dependencies", default_args=default_args, schedule_interval="@daily")
run_this = BashOperator(
    task_id='bashOp_install_dependencies',
    bash_command='pip3 install -r /opt/airflow/dags/repo/requirements.txt && python3 -m pip install python-dotenv',
    dag=dag
)

run_this