from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def test_email():
    raise Exception("This is a test exception")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["chienhua.hsu@tri.org.tw"],
    "email_on_failure": True,
    "email_on_retry": False,
    "tags": ["example"],
}

dag = DAG(
    "test_email_dag",
    default_args=default_args,
    description="A simple DAG to test email",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

test_task = PythonOperator(
    task_id="test_email_task",
    python_callable=test_email,
    dag=dag,
)
