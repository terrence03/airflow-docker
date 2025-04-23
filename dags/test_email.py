from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email import EmailOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="send_email_via_smtp",
    default_args=default_args,
    description="使用 SMTP 發送 Email",
    schedule_interval=None,  # 手動觸發
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["email", "smtp"],
)

send_email = EmailOperator(
    task_id="send_test_email",
    to="chienhua.hsu@tri.org.tw",  # 替換成收件者
    subject="Airflow 測試郵件",
    html_content="""
    <h3>這是一封由 Airflow 發出的測試郵件</h3>
    <p>內容可以使用 HTML 格式。</p>
    """,
    dag=dag,
)

send_email
