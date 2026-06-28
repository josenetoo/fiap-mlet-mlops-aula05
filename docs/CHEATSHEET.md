# 📋 CHEATSHEET - Aula 05: Orquestração com Airflow

## Instalação (Docker Compose — método do curso)

```bash
# Baixar o docker-compose.yaml oficial do Airflow 2.8.0
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml'

# Criar pastas e definir o UID do host (Linux/Mac)
mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env

# Inicializar banco e usuário admin (roda uma vez)
docker compose up airflow-init

# Subir todos os serviços
docker compose up -d

# Conferir status
docker compose ps
```

> UI em http://localhost:8080 — login padrão do compose oficial: `airflow` / `airflow`.

> 💡 Os comandos `airflow ...` abaixo rodam **dentro** do container. Prefixe com
> `docker compose exec airflow-scheduler` (ex.: `docker compose exec airflow-scheduler airflow dags list`).

## Conceitos Básicos

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Definir DAG
dag = DAG(
    'my_dag',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    catchup=False
)

# Definir task
def my_function():
    print("Hello Airflow!")

task = PythonOperator(
    task_id='my_task',
    python_callable=my_function,
    dag=dag
)
```

## DAG Completo de ML

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_data(**context):
    # Extrair dados
    context['ti'].xcom_push(key='data', value={'samples': 1000})

def train_model(**context):
    # Pegar dados do XCom
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract', key='data')
    # Treinar modelo
    pass

dag = DAG('ml_pipeline', start_date=datetime(2026, 1, 1))

extract = PythonOperator(task_id='extract', python_callable=extract_data, dag=dag)
train = PythonOperator(task_id='train', python_callable=train_model, dag=dag)

extract >> train  # Dependência
```

## Comandos CLI

```bash
# Listar DAGs
airflow dags list

# Testar task
airflow tasks test my_dag my_task 2026-01-01

# Trigger DAG
airflow dags trigger my_dag

# Ver logs
airflow tasks logs my_dag my_task 2026-01-01

# Pausar/Despausar DAG
airflow dags pause my_dag
airflow dags unpause my_dag
```

## XCom (Comunicação entre Tasks)

```python
# Push
context['ti'].xcom_push(key='accuracy', value=0.95)

# Pull
ti = context['ti']
accuracy = ti.xcom_pull(task_ids='train_model', key='accuracy')
```

## Schedule Intervals

```python
'@once'      # Uma vez
'@hourly'    # A cada hora
'@daily'     # Diariamente
'@weekly'    # Semanalmente
'@monthly'   # Mensalmente
'0 0 * * *'  # Cron: meia-noite todo dia
```

## Operadores Comuns

```python
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Python
PythonOperator(task_id='task', python_callable=func)

# Bash
BashOperator(task_id='task', bash_command='echo "Hello"')

# Empty (placeholder)
EmptyOperator(task_id='start')
```

## Dependências

```python
# Linear
task1 >> task2 >> task3

# Paralelo
task1 >> [task2, task3] >> task4

# Múltiplas
task1.set_downstream(task2)
task1.set_upstream(task0)
```

## Retry e Timeout

```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}
```
