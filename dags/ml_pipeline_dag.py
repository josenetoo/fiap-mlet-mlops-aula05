"""Pipeline de ML orquestrado com Airflow.

Fluxo: ingest -> validate -> train -> evaluate -> deploy

Boas práticas aplicadas:
- Retries com backoff exponencial
- Timeout em cada task
- on_failure_callback para alertas
- catchup=False
- Tasks idempotentes
"""
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ml_pipeline.tasks.ml_tasks import (
    deploy_model,
    evaluate_model,
    ingest_data,
    train_model,
    validate_data,
)

logger = logging.getLogger(__name__)


def alert_failure(context):
    """Callback chamado quando task falha (substitua por Slack/PagerDuty em prod)."""
    ti = context["task_instance"]
    logger.error(
        "🚨 ALERTA: Task %s falhou na DAG %s (run %s). Log: %s",
        ti.task_id, ti.dag_id, ti.run_id, ti.log_url,
    )


default_args = {
    "owner": "fiap",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": alert_failure,
}

with DAG(
    dag_id="ml_pipeline",
    description="Pipeline de ML end-to-end com boas práticas",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,             # apenas manual; em prod: "0 2 * * *"
    catchup=False,
    tags=["fiap", "ml", "aula05"],
) as dag:

    t_ingest = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t_train = PythonOperator(task_id="train_model", python_callable=train_model)
    t_evaluate = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)
    t_deploy = PythonOperator(task_id="deploy_model", python_callable=deploy_model)

    t_ingest >> t_validate >> t_train >> t_evaluate >> t_deploy
