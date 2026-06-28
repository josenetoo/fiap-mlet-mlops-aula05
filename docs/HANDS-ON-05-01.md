# 🎬 Vídeo 5.1 - Introdução ao Apache Airflow

**Aula**: 5 - Orquestração com Airflow  
**Vídeo**: 5.1  
**Temas**: Orquestração; DAGs; Tasks; Setup com Docker Compose

---

## 🚀 Sobre Este Vídeo

> **"Airflow é o `cron` que sabe lidar com dependências, retry e observabilidade."**

### O que você vai fazer:

| Etapa | Descrição |
|-------|-----------|
| **Subir Airflow** | Via Docker Compose |
| **UI** | Acessar `http://localhost:8080` |
| **Primeira DAG** | Hello World com 3 tasks |
| **Trigger** | Executar e ver na UI |

### Pré-requisitos

| Requisito | Como verificar |
|-----------|----------------|
| Aula 04 concluída | Docker rodando |
| Docker Desktop | `docker ps` (sem erro) |
| Docker Compose v2 | `docker compose version` |
| 4 GB RAM livres | Airflow puxa ~3 GB |
| Porta 8080 livre | `lsof -i :8080` |

---

## 📚 Parte 1: O Problema da Orquestração

### Passo 1: Sem Airflow

```bash
# A cada noite, alguém roda:
python extract_data.py        # 30 min
python validate_data.py       # esqueceu? quebra silenciosa
python train_model.py         # 2 h
python evaluate_model.py
python deploy_model.py
```

**Problemas:**

| Problema | Resultado |
|----------|-----------|
| Ordem manual | Erro humano |
| Sem retry | Falhou? Recomeça tudo |
| Sem logs centralizados | Difícil debugar |
| Sem agendamento | Esqueceu de rodar = 1 dia perdido |
| Sem visibilidade | Não sabe se rodou |

---

### Passo 2: Com Airflow

```python
extract >> validate >> train >> evaluate >> deploy
```

**Ganha:**

| Recurso | Benefício |
|---------|-----------|
| **Schedule** | Roda automaticamente (cron, intervalo) |
| **Dependências** | Ordem garantida |
| **Retry** | Task falhou? Retenta sozinha |
| **Logs centralizados** | Tudo na UI web |
| **Monitoramento visual** | Verde = ok, vermelho = falha |

> 💡 **Ponto-chave**: Airflow é a **Jenkins** do ML. Mesma família de ferramenta (Jenkins, GitHub Actions, Step Functions).

---

## 🐳 Parte 2: Subir Airflow

### Passo 3: Criar Pasta e Baixar Compose

**Linux/Mac:**
```bash
mkdir -p ~/fiap-mlops/aula05
cd ~/fiap-mlops/aula05
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml'
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Path "$HOME\fiap-mlops\aula05" -Force
cd "$HOME\fiap-mlops\aula05"
Invoke-WebRequest -Uri "https://airflow.apache.org/docs/apache-airflow/2.8.0/docker-compose.yaml" -OutFile docker-compose.yaml
```

**Resultado esperado:** Arquivo `docker-compose.yaml` baixado (~5 KB).

---

### Passo 4: Criar Pastas e Variáveis

**Linux/Mac:**
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env
```

**Windows (PowerShell):**
```powershell
New-Item -ItemType Directory -Path dags, logs, plugins, config -Force
"AIRFLOW_UID=50000" | Out-File -FilePath .env -Encoding ascii
```

> ⚠️ **IMPORTANTE**: No Mac/Linux, o `AIRFLOW_UID` precisa ser o seu user ID para evitar erros de permissão. No Windows, use `50000` (padrão).

> 🔍 **Nos bastidores**: os processos dentro do container rodam com esse UID. Como `dags/`, `logs/` e `plugins/` são *bind mounts* (pastas do host montadas no container), o UID de quem escreve precisa bater com o dono dos arquivos no host. UID divergente = arquivos de log que você não consegue ler/apagar e o clássico `Permission denied`. No Windows o mount passa por uma camada de tradução, por isso o valor fixo `50000` resolve.

✅ Estrutura pronta.

---

### Passo 5: Inicializar Banco do Airflow

**Linux/Mac e Windows:**
```bash
docker compose up airflow-init
```

**Resultado esperado:**
```
airflow-init-1  | User "airflow" created with role "Admin"
airflow-init-1  | 2.8.0
airflow-init-1 exited with code 0
```

> 💡 Esse comando cria o banco e o usuário admin. Roda uma vez só.

> 🔍 **Nos bastidores**: o `airflow-init` é um container efêmero que aplica as migrations do schema no Postgres (tabelas de DAGs, runs, XCom, etc.) e semeia o usuário admin. Ele sobe, faz o trabalho e sai com código 0 — não é um serviço que fica de pé. Rodar de novo é seguro (as migrations são idempotentes), mas só é necessário após zerar o banco.

✅ Banco inicializado.

---

### Passo 6: Subir Todos os Serviços

**Linux/Mac e Windows:**
```bash
docker compose up -d
```

**Aguardar ~2 min**, depois verificar:

```bash
docker compose ps
```

**Resultado esperado:**
```
NAME                          STATUS
airflow-airflow-scheduler-1   Up 2 minutes (healthy)
airflow-airflow-webserver-1   Up 2 minutes (healthy)
airflow-postgres-1            Up 2 minutes (healthy)
airflow-redis-1               Up 2 minutes (healthy)
```

✅ Airflow rodando.

---

### Passo 7: Acessar UI

**Abrir no browser**: http://localhost:8080

**Login:**
- Usuário: `airflow`
- Senha: `airflow`

**Resultado esperado:** Tela de DAGs (vazia ou com exemplos do Airflow).

> 💡 **Ponto-chave**: A UI do Airflow tem cara de Jenkins. Lista de jobs (DAGs), histórico de execuções, logs por task.

✅ UI acessível.

---

## 📝 Parte 3: Primeira DAG

### Passo 8: Criar Hello World

**Criar `dags/hello_world_dag.py`:**

```python
"""Primeira DAG: 3 tasks em sequência."""
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
    schedule=None,           # apenas manual
    catchup=False,
    tags=["fiap", "aula05"],
) as dag:

    t0 = PythonOperator(task_id="hello", python_callable=hello)
    t1 = PythonOperator(task_id="task_a", python_callable=task_a)
    t2 = PythonOperator(task_id="task_b", python_callable=task_b)

    t0 >> t1 >> t2
```

---

### Passo 9: Aguardar Airflow Detectar

O scheduler verifica a pasta `dags/` a cada **30 segundos**.

> 🔍 **Nos bastidores**: existe um processo dedicado (o *DAG processor*) que reexecuta cada arquivo `.py` da pasta `dags/` em intervalos regulares para extrair os objetos DAG. Duas consequências práticas: (1) o arquivo da DAG roda toda hora, então código pesado no topo do módulo — conexão a banco, download, treino — trava o parsing e atrasa **todas** as DAGs; deixe o topo só com imports e definição. (2) Se o arquivo tem erro de import, a DAG simplesmente não aparece na UI, sem alarde — o erro fica no log do scheduler.

**Validar:**

**Linux/Mac e Windows:**
```bash
sleep 30
docker compose exec airflow-scheduler airflow dags list | grep hello_world
```

**Resultado esperado:**
```
hello_world | /opt/airflow/dags/hello_world_dag.py | airflow | False
```

✅ DAG detectada.

---

### Passo 10: Trigger na UI

**Na UI (http://localhost:8080):**

1. Encontre `hello_world` na lista
2. Clique no **toggle azul** à esquerda (ativa a DAG)
3. Clique no botão **▶️ Play** (canto direito)
4. Selecione **Trigger DAG**

**Resultado esperado:** Bolinha verde aparece em cada task em sequência.

---

### Passo 11: Ver Logs da Task

**Na UI:**

1. Clique na execução (linha verde)
2. Clique em uma task (ex: `hello`)
3. Aba **Logs**

**Resultado esperado:**
```
[2026-06-26 10:00:00 UTC] {logging_mixin.py:188} INFO - 👋 Hello from Airflow!
[2026-06-26 10:00:00 UTC] {python.py:194} INFO - Done. Returned value was: None
```

✅ Logs centralizados visíveis.

---

### Passo 12: Visualizar o Grafo

**Na UI:**

1. Clique na DAG `hello_world`
2. Aba **Graph**

**Resultado esperado:** Diagrama com 3 caixinhas verdes em sequência: `hello → task_a → task_b`.

> 💡 **Ponto-chave**: Esse grafo é gerado **automaticamente** pelas dependências (`>>`). Não precisa desenhar.

✅ DAG visualizada.

---

## 🛑 Parte 4: Parar e Limpar

### Passo 13: Parar (sem perder dados)

**Linux/Mac e Windows:**
```bash
docker compose stop
```

**Para subir de novo:** `docker compose start`.

---

### Passo 14: Remover Tudo

Só faça se quiser começar do zero:

**Linux/Mac e Windows:**
```bash
docker compose down -v
```

> ⚠️ `-v` apaga volumes (banco e logs). Cuidado.

✅ Ambiente limpo.

---

## 🔧 Troubleshooting

| Erro | Causa | Solução |
|------|-------|---------|
| `Bind for 0.0.0.0:8080 failed` | Porta 8080 ocupada | `lsof -i :8080` e matar processo |
| Containers reiniciando | Pouca RAM (precisa ~4GB) | Aumentar memória do Docker Desktop |
| DAG não aparece na UI | Erro de import no arquivo | Ver logs: `docker compose logs airflow-scheduler` |
| Login não funciona | `airflow-init` não rodou | Rodar `docker compose up airflow-init` |
| Permission denied em logs/ | UID errado | `echo "AIRFLOW_UID=$(id -u)" > .env` |
| DAG roda mas marca falha | `start_date` no futuro | Usar data passada (`datetime(2026, 1, 1)`) |

---

**FIM DO VÍDEO 5.1** ✅
