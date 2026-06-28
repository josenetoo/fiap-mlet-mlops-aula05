# 🎬 Vídeo 5.2 - Pipeline de ML com Airflow

**Aula**: 5 - Orquestração com Airflow  
**Vídeo**: 5.2  
**Temas**: DAG de ML; PythonOperator; XCom; Dependências

---

## 🚀 Sobre Este Vídeo

> **"Vamos pegar o pipeline da Aula 01 (ingest → validate → train → evaluate → deploy) e transformar em DAG de verdade."**

### O que você vai fazer:

| Etapa | Descrição |
|-------|-----------|
| **DAG de ML** | Pipeline completo em Airflow |
| **PythonOperator** | Chamar funções Python |
| **XCom** | Passar dados entre tasks |
| **Dependências** | Encadear corretamente |

### Pré-requisitos

| Requisito | Como verificar |
|-----------|----------------|
| Vídeo 5.1 concluído | Airflow rodando, `hello_world` funcionou |
| Container Airflow up | `docker compose ps` mostra healthy |
| Pasta `dags/` montada | `ls dags/` no host |

---

## 📚 Parte 1: De Script para DAG

### Passo 1: Estrutura do Pipeline ML

Recapitulando a Aula 01:

```
Ingest → Validate → Train → Evaluate → Deploy
```

Em Airflow, **cada função vira uma task**:

```
ingest_data → validate_data → train_model → evaluate_model → deploy_model
```

---

### Passo 2: O Que NÃO Passar pelo XCom

> ⚠️ **REGRA DE OURO**: XCom serializa em JSON e é guardado no metadados do Airflow. **NUNCA** passe DataFrames inteiros ou objetos grandes.

**Ruim:**
```python
return df  # DataFrame de 5GB → trava o banco
```

**Bom:**
```python
return "/tmp/data.csv"  # caminho do arquivo
```

> 🔍 **Nos bastidores**: cada `return` de uma task é serializado (JSON por padrão) e gravado como uma linha na tabela `xcom` do banco de metadados. Por isso o limite: dado grande incha o Postgres e cada leitura vira um round-trip ao banco. O padrão correto é o XCom carregar **referências** (caminho, ID, métrica) e o dado pesado viver fora — disco compartilhado, S3, banco. Repare que neste pipeline o `model.pkl` e o `test.npz` ficam em `/tmp/ml_pipeline` e só o **caminho** trafega pelo XCom.

---

## 🛠️ Parte 2: Criar a DAG

### Passo 3: Estrutura de Pastas

**Linux/Mac:**
```bash
cd ~/fiap-mlops/aula05
mkdir -p dags/ml_pipeline/tasks
ls dags/
```

**Windows (PowerShell):**
```powershell
cd "$HOME\fiap-mlops\aula05"
New-Item -ItemType Directory -Path dags\ml_pipeline\tasks -Force
Get-ChildItem dags\
```

✅ Estrutura criada.

---

### Passo 4: Funções das Tasks

**Criar `dags/ml_pipeline/tasks/__init__.py`** (vazio):

**Linux/Mac:**
```bash
touch dags/ml_pipeline/__init__.py
touch dags/ml_pipeline/tasks/__init__.py
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType File -Path dags\ml_pipeline\__init__.py, dags\ml_pipeline\tasks\__init__.py -Force
```

---

### Passo 5: Criar Tasks de ML

**Criar `dags/ml_pipeline/tasks/ml_tasks.py`:**

```python
"""Tasks do pipeline de ML."""
import json
import logging
from pathlib import Path

import joblib
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

DATA_DIR = Path("/tmp/ml_pipeline")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def ingest_data():
    """Carrega Iris e salva em CSV."""
    logger.info("📥 Ingerindo dados...")
    X, y = load_iris(return_X_y=True)

    import numpy as np
    data = np.column_stack([X, y])
    path = DATA_DIR / "raw.csv"
    np.savetxt(path, data, delimiter=",", header="sl,sw,pl,pw,target", comments="")

    logger.info(f"✅ {len(data)} linhas salvas em {path}")
    return str(path)


def validate_data(**context):
    """Valida o CSV gerado."""
    import numpy as np
    path = context["ti"].xcom_pull(task_ids="ingest_data")
    logger.info(f"🔍 Validando {path}")

    data = np.loadtxt(path, delimiter=",", skiprows=1)
    assert data.shape == (150, 5), f"Shape errado: {data.shape}"
    assert not np.isnan(data).any(), "NaN encontrado"

    logger.info(f"✅ Dados válidos: {data.shape}")
    return path


def train_model(**context):
    """Treina RandomForest."""
    import numpy as np
    path = context["ti"].xcom_pull(task_ids="validate_data")
    data = np.loadtxt(path, delimiter=",", skiprows=1)

    X, y = data[:, :4], data[:, 4].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42).fit(X_tr, y_tr)

    model_path = DATA_DIR / "model.pkl"
    joblib.dump(model, model_path)
    logger.info(f"✅ Modelo salvo em {model_path}")

    # Salvar X_te e y_te para próxima task
    test_path = DATA_DIR / "test.npz"
    np.savez(test_path, X_te=X_te, y_te=y_te)

    return {"model_path": str(model_path), "test_path": str(test_path)}


def evaluate_model(**context):
    """Avalia o modelo."""
    import numpy as np
    paths = context["ti"].xcom_pull(task_ids="train_model")

    model = joblib.load(paths["model_path"])
    test = np.load(paths["test_path"])
    X_te, y_te = test["X_te"], test["y_te"]

    accuracy = accuracy_score(y_te, model.predict(X_te))
    logger.info(f"📊 Accuracy: {accuracy:.3f}")

    threshold = 0.9
    if accuracy < threshold:
        raise ValueError(f"Accuracy {accuracy:.3f} < threshold {threshold}")

    return {"accuracy": float(accuracy), "model_path": paths["model_path"]}


def deploy_model(**context):
    """'Deploya' (copia para pasta de produção)."""
    import shutil
    result = context["ti"].xcom_pull(task_ids="evaluate_model")

    prod_path = DATA_DIR / "production" / "model.pkl"
    prod_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(result["model_path"], prod_path)

    # Salvar metadados do deploy
    meta = {"accuracy": result["accuracy"], "deployed_at": str(context["ts"])}
    (DATA_DIR / "production" / "metadata.json").write_text(json.dumps(meta, indent=2))

    logger.info(f"🚀 Modelo deployado em {prod_path}")
    return str(prod_path)
```

---

### Passo 6: Criar a DAG

**Criar `dags/ml_pipeline_dag.py`:**

```python
"""Pipeline de ML orquestrado com Airflow."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from ml_pipeline.tasks.ml_tasks import (
    ingest_data,
    validate_data,
    train_model,
    evaluate_model,
    deploy_model,
)

default_args = {
    "owner": "fiap",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="ml_pipeline",
    description="Pipeline de ML end-to-end",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fiap", "ml"],
) as dag:

    t_ingest = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data)
    t_train = PythonOperator(task_id="train_model", python_callable=train_model)
    t_evaluate = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)
    t_deploy = PythonOperator(task_id="deploy_model", python_callable=deploy_model)

    t_ingest >> t_validate >> t_train >> t_evaluate >> t_deploy
```

---

### Passo 7: Instalar scikit-learn no Airflow

O container do Airflow não tem `scikit-learn` por padrão. Vamos adicionar via variável de ambiente.

**Linux/Mac e Windows** — editar `docker-compose.yaml`:

Procure a seção `x-airflow-common` e adicione (ou ajuste) a variável de ambiente:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: 'scikit-learn==1.3.0 joblib==1.3.2'
```

**Reiniciar:**

```bash
docker compose down
docker compose up -d
```

> 💡 Em produção real, você criaria uma **imagem custom** do Airflow com as deps. Para a aula, isso basta.

> 🔍 **Nos bastidores**: o `_PIP_ADDITIONAL_REQUIREMENTS` roda um `pip install` no **boot de cada container** (webserver, scheduler, worker). Funciona para demo, mas tem custo: cada restart fica mais lento, não há cache reprodutível e uma versão nova publicada no PyPI pode mudar o ambiente sem você mexer em nada. Por isso a recomendação de "imagem custom" — você fixa as deps em build time (`Dockerfile` com `FROM apache/airflow`), versiona e ganha startup rápido e determinístico.

> ⚠️ **Atenção ao filesystem compartilhado**: passar caminhos via XCom (`/tmp/ml_pipeline/...`) só funciona porque, nesta configuração, as tasks compartilham o mesmo disco. Se os workers forem distribuídos em máquinas diferentes, `/tmp` de um worker não é o do outro — aí o dado precisa ir para um storage comum (S3, NFS, banco). É o motivo de "passar referência" pressupor um local que **todos** os workers enxergam.

✅ Dependências instaladas.

---

## ▶️ Parte 3: Executar Pipeline

### Passo 8: Trigger a DAG

**Na UI (http://localhost:8080):**

1. Aguardar a DAG `ml_pipeline` aparecer (até 30s após reiniciar)
2. Ativar com o toggle azul
3. Clicar em **▶️ Play** → **Trigger DAG**

**Resultado esperado (na aba Grid):**
```
ingest_data       ✅
validate_data     ✅
train_model       ✅
evaluate_model    ✅
deploy_model      ✅
```

✅ Pipeline executou.

---

### Passo 9: Ver Métricas Logadas

**Na UI:**

1. Click na execução
2. Click em `evaluate_model`
3. Aba **Logs**

**Resultado esperado:**
```
INFO - 📊 Accuracy: 0.967
INFO - Done. Returned value: {'accuracy': 0.967, ...}
```

✅ Modelo avaliado.

---

### Passo 10: Validar Deploy

> ⚠️ **Container certo**: este `docker-compose.yaml` usa **CeleryExecutor**, então as tasks rodam no container `airflow-worker` — não no `airflow-scheduler`. Os arquivos gerados pelas tasks (em `/tmp`) ficam no worker. Por isso a validação aponta para `airflow-worker`.

**Linux/Mac:**
```bash
docker compose exec airflow-worker ls -la /tmp/ml_pipeline/production/
docker compose exec airflow-worker cat /tmp/ml_pipeline/production/metadata.json
```

**Windows (PowerShell):**
```powershell
docker compose exec airflow-worker ls -la /tmp/ml_pipeline/production/
docker compose exec airflow-worker cat /tmp/ml_pipeline/production/metadata.json
```

**Resultado esperado:**
```
model.pkl
metadata.json

{
  "accuracy": 0.9666666666666667,
  "deployed_at": "2026-06-26T10:00:00+00:00"
}
```

> 🔍 **Nos bastidores**: `/tmp` é local de cada container. Se você executar o `ls` no `airflow-scheduler`, vai ver `No such file or directory` — o deploy aconteceu no `/tmp` do worker, não no do scheduler. É a mesma razão pela qual, com vários workers em máquinas diferentes, passar caminho de arquivo via XCom só funciona se o destino for um storage compartilhado (S3, NFS, banco) que todos enxergam.

✅ Modelo deployado dentro do container.

---

## 🔬 Parte 4: Ver XCom Funcionando

### Passo 11: Inspecionar XCom

**Na UI:**

1. Click na execução
2. Click em `train_model`
3. Aba **XCom**

**Resultado esperado:**
```
key            value
return_value   {"model_path": "/tmp/ml_pipeline/model.pkl", "test_path": "/tmp/ml_pipeline/test.npz"}
```

> 💡 **Ponto-chave**: O `return` da função vira automaticamente um XCom (`return_value`). A próxima task pega com `xcom_pull(task_ids="...")`.

> 🔍 **Nos bastidores**: `return_value` é só a *key* default que o Airflow usa quando você retorna algo da função. Dá para ter vários XComs nomeados na mesma task com `xcom_push(key="acc", value=...)` e ler o que quiser com `xcom_pull(task_ids="...", key="acc")`. Como o XCom é endereçado por `task_id` + `key`, ele cria um acoplamento explícito entre tasks: a leitora precisa saber o nome de quem produziu. É o que torna o grafo de dados rastreável, mas também por que renomear um `task_id` quebra quem lê dele.

✅ XCom em ação.

---

## 🔧 Troubleshooting

| Erro | Causa | Solução |
|------|-------|---------|
| `ModuleNotFoundError: sklearn` | Falta deps no container | Adicionar `_PIP_ADDITIONAL_REQUIREMENTS` |
| DAG não aparece | Erro de import | `docker compose logs airflow-scheduler \| grep ERROR` |
| `XCom size limit exceeded` | Passou dado grande | Passar **path** de arquivo, não dado |
| Task `evaluate_model` falha | Accuracy < threshold | Diminuir threshold (didático) |
| Container reinicia em loop | Conflito de versão sklearn | Fixar `scikit-learn==1.3.0` |
| `Permission denied` em /tmp | Mount errado | Verificar volumes no `docker-compose.yaml` |

---

**FIM DO VÍDEO 5.2** ✅
