"""Primeira DAG: 3 tasks em sequência (Hello World)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("👋 Hello from Airflow!")


def task_a():
    print("Executando Task A...")


def task_b():
    print("Executando Task B...")


default_args = {
    "owner": "fiap",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world",
    description="Primeira DAG do curso",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fiap", "aula05"],
) as dag:

    t0 = PythonOperator(task_id="hello", python_callable=hello)
    t1 = PythonOperator(task_id="task_a", python_callable=task_a)
    t2 = PythonOperator(task_id="task_b", python_callable=task_b)

    t0 >> t1 >> t2
