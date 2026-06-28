# 🎬 Vídeo 5.3 - Monitoramento e Boas Práticas

**Aula**: 5 - Orquestração com Airflow  
**Vídeo**: 5.3  
**Temas**: Retry; Timeout; Alertas; Schedule; Idempotência

---

## 🚀 Sobre Este Vídeo

> **"Em DevOps, deploy sem retry e alerta é amador. Em Airflow é a mesma coisa."**

### O que você vai fazer:

| Etapa | Descrição |
|-------|-----------|
| **Retry** | Tentar de novo automaticamente |
| **Timeout** | Não deixar task pendurada |
| **Alertas** | Notificar quando falhar |
| **Schedule** | Agendamento cron-like |
| **Idempotência** | Rodar 2x sem quebrar |

### Pré-requisitos

| Requisito | Como verificar |
|-----------|----------------|
| Vídeo 5.2 concluído | `ml_pipeline` DAG funcionando |
| Airflow rodando | `docker compose ps` |

---

## 📚 Parte 1: Retry Automático

### Passo 1: Por Que Retry?

| Cenário | Sem retry | Com retry |
|---------|-----------|-----------|
| API externa fora do ar 30s | Pipeline quebra | Espera e tenta de novo |
| Erro de rede transitório | Manual: rerun | Automático |
| Banco lento momentaneamente | Falha | Retry com backoff |

> 💡 **Ponto-chave**: ~70% das falhas em pipelines são transitórias. Retry resolve sem intervenção humana.

---

### Passo 2: Configurar Retry

**Atualizar `dags/ml_pipeline_dag.py`:**

```python
default_args = {
    "owner": "fiap",
    "retries": 3,                           # tenta até 3 vezes
    "retry_delay": timedelta(minutes=2),    # espera 2 min entre tentativas
    "retry_exponential_backoff": True,      # 2, 4, 8 min (cresce)
    "max_retry_delay": timedelta(minutes=10),
}
```

> 💡 **Backoff exponencial**: mesmo conceito do `kubectl` quando pod falha. Espera cada vez mais.

> 🔍 **Nos bastidores**: sem backoff, as 3 tentativas batem na mesma janela de tempo — se a causa foi uma dependência fora do ar, você só empilha falhas. O backoff (2 → 4 → 8 min) dá tempo do recurso se recuperar, e o `max_retry_delay` impõe um teto para o intervalo não crescer indefinidamente. O `retry_delay` conta a partir do **fim** da tentativa que falhou, não do início da task.

---

### Passo 3: Simular Falha e Ver Retry

**Adicionar ao `ml_tasks.py` (apenas para demo):**

```python
import random

def flaky_task(**context):
    """Task que falha 50% das vezes para demonstrar retry."""
    if random.random() < 0.5:
        raise RuntimeError("Falha simulada!")
    print("✅ Sucesso!")
```

**Na DAG**, adicione esta task antes do `t_deploy`:

```python
from ml_pipeline.tasks.ml_tasks import flaky_task

t_flaky = PythonOperator(
    task_id="flaky_task",
    python_callable=flaky_task,
    retries=5,                       # sobrepõe default
    retry_delay=timedelta(seconds=10),
)

t_evaluate >> t_flaky >> t_deploy
```

---

### Passo 4: Executar e Observar

**Trigger a DAG na UI**. Observe na aba **Grid**:

**Resultado esperado:**
```
flaky_task   🟡 up_for_retry  (1/5)
              ⏳ aguardando 10s
flaky_task   🟡 up_for_retry  (2/5)
              ⏳ aguardando 20s
flaky_task   ✅ success       (3/5)
```

✅ Retry com backoff funcionando.

---

## ⏱️ Parte 2: Timeout

### Passo 5: Limitar Duração

**Em uma task específica:**

```python
t_train = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    execution_timeout=timedelta(hours=1),  # mata após 1h
)
```

| Cenário | Sem timeout | Com timeout |
|---------|-------------|-------------|
| Bug em loop infinito | Task fica pendurada | Mata após N min |
| Modelo gigante demais | Bloqueia worker | Falha rápida |
| API externa travada | Espera infinita | Mata e retry |

> ⚠️ **IMPORTANTE**: Sem `execution_timeout`, task pode ficar **dias** rodando. Sempre defina.

> 🔍 **Nos bastidores**: ao estourar o tempo, o Airflow envia um `SIGTERM` para o processo da task. Código Python "comportado" para; mas chamada bloqueante em C, I/O travado ou loop sem ponto de interrupção pode ignorar o sinal e só morrer no `SIGKILL` posterior. Por isso, para chamadas externas, combine `execution_timeout` (rede de segurança do orquestrador) com um timeout no próprio cliente (`requests.get(..., timeout=...)`), que falha de forma limpa e ainda permite retry.

---

## 🔔 Parte 3: Alertas

### Passo 6: `on_failure_callback`

**Adicionar ao `ml_pipeline_dag.py`:**

```python
import logging

def alert_failure(context):
    """Callback chamado quando uma task falha."""
    ti = context["task_instance"]
    logging.error(
        f"🚨 ALERTA: Task {ti.task_id} falhou na DAG {ti.dag_id} "
        f"(run {ti.run_id}). Log: {ti.log_url}"
    )
    # Em produção: chamar API do Slack/PagerDuty/email
    # requests.post(SLACK_WEBHOOK, json={"text": f"Task {ti.task_id} falhou!"})


default_args = {
    "owner": "fiap",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_failure,
}
```

---

### Passo 7: Testar Alerta

Para o alerta disparar, é preciso **forçar uma falha** numa task. Fazemos isso com duas alterações **temporárias** (revertidas ao final do passo):

**Alteração 1 — forçar a falha** em `dags/ml_pipeline/tasks/ml_tasks.py`, dentro de `evaluate_model`:

```python
    threshold = 0.99   # TEMPORÁRIO: era 0.9. A accuracy real (~0.96) fica abaixo, então a task falha de propósito
```

**Alteração 2 — falhar na hora** em `dags/ml_pipeline_dag.py`, no `default_args`:

```python
    "retries": 0,      # TEMPORÁRIO: era 3. Sem retry, a task falha de primeira e o callback dispara na hora
```

> 💡 **Por que mexer no `retries`**: o `on_failure_callback` só dispara na **falha definitiva** (após esgotar os retries). Se deixar `retries=3` + backoff, o alerta só sairia depois de ~14 min de tentativas (esperas de 2 → 4 → 8 min). Com `retries=0`, a falha é imediata. Se você alterar **só o threshold**, a DAG fica em `up_for_retry` e parece "ainda rodando" — é só o ciclo de retry em andamento.

**Trigger a DAG.** O `evaluate_model` fica vermelho.

**Onde ver o alerta:** o `logger.error` dentro do callback é gravado no **log da própria task**, não no stdout do container. Então:

**Opção 1 — UI do Airflow (recomendado):**
1. Clique na execução que falhou → task `evaluate_model` → aba **Logs**
2. Procure por `ALERTA`

**Opção 2 — nos arquivos de log (pasta `logs/` montada no host):**
```bash
grep -r "ALERTA" logs/
```

**Resultado esperado:**
```
{ml_pipeline_dag.py:32} ERROR - 🚨 ALERTA: Task evaluate_model falhou na DAG ml_pipeline (run ...). Log: http://localhost:8080/log?...
```

> ⚠️ **Por que `docker compose logs airflow-worker | grep ALERTA` não mostra nada**: o callback roda dentro do contexto de logging da task, que escreve em arquivo de log por task (handler de arquivo do Airflow) — e não na saída padrão do worker. Procurar no stdout do container retorna vazio mesmo com o alerta tendo disparado.

**Reverter as alterações temporárias** (importante, para o pipeline voltar a passar):
- `threshold = 0.99` → volta para `0.9` em `ml_tasks.py`
- `"retries": 0` → volta para `3` em `ml_pipeline_dag.py`

> 💡 **Ponto-chave**: Em produção, troque o `logging.error` por chamada HTTP para Slack/PagerDuty. O gatilho é o mesmo; muda só o destino do alerta.

✅ Alertas configurados.

---

## 📅 Parte 4: Schedule

### Passo 8: Agendamento

```python
with DAG(
    dag_id="ml_pipeline",
    schedule="0 2 * * *",       # cron: diário às 2h
    start_date=datetime(2026, 1, 1),
    catchup=False,              # NÃO roda execuções passadas
    ...
)
```

**Outras opções:**

| Schedule | Significado |
|----------|-------------|
| `"@daily"` | Todo dia à meia-noite |
| `"@hourly"` | A cada hora |
| `"@weekly"` | Toda segunda à meia-noite |
| `"0 */6 * * *"` | A cada 6 horas |
| `timedelta(hours=4)` | A cada 4 horas |
| `None` | Apenas manual |

> ⚠️ **CRÍTICO**: `catchup=False` evita Airflow rodar **todas** as execuções desde `start_date`. Default é `True`, o que pode disparar centenas de runs.

> 🔍 **Nos bastidores — o conceito que mais confunde**: o Airflow não pensa em "rodar agora". Ele pensa em **intervalos de dados** (*data intervals*). Cada run cobre um intervalo fechado e dispara **no fim** dele. Com `@daily`, o run do dia 10 representa os dados de 10/00:00 a 11/00:00 e só inicia à meia-noite do dia 11 — por isso a `logical_date` (antiga `execution_date`) parece "um período atrasada": ela marca o **início do intervalo**, não o momento da execução. Daí também o papel do `catchup`: se `start_date` é antigo e `catchup=True`, o scheduler entende que existem N intervalos passados não processados e dispara todos de uma vez (backfill). `catchup=False` faz ele pular direto para o intervalo mais recente.

---

### Passo 9: Schedule no Fuso Certo

Airflow roda em **UTC** por padrão. Para São Paulo:

```python
import pendulum

with DAG(
    dag_id="ml_pipeline",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    ...
)
```

> 💡 **Ponto-chave**: Sem timezone, "2h" do código vira 11h em SP (UTC-3). Confunde quem opera.

> 🔍 **Nos bastidores**: o `start_date` com `tz` define em que fuso o cron é interpretado para *agendar* os disparos. Já a `logical_date` que aparece na UI e nos logs continua sendo armazenada em UTC — Airflow normaliza tudo para UTC internamente e só converte na exibição. Um detalhe de operação: em fusos com horário de verão, agendamentos próximos da virada podem pular ou repetir um intervalo; por isso muita equipe padroniza o cron em UTC e faz a conta de cabeça, justamente para fugir dessa ambiguidade.

---

## 🔁 Parte 5: Idempotência

### Passo 10: O Que É?

> **Idempotente** = rodar 1x ou 100x dá o **mesmo resultado**.

| Não-idempotente | Idempotente |
|-----------------|-------------|
| `INSERT` em tabela (duplica) | `INSERT ... ON CONFLICT` |
| `mkdir` (falha 2ª vez) | `mkdir -p` |
| Acumular em variável global | Sempre começar do zero |

**Por que importa?** Airflow pode re-executar a mesma task (retry, backfill). Precisa funcionar igual.

---

### Passo 11: Aplicar no Pipeline

**No `ml_tasks.py`**, garantir que cada task pode rodar várias vezes:

```python
def deploy_model(**context):
    prod_dir = DATA_DIR / "production"
    prod_dir.mkdir(parents=True, exist_ok=True)  # 👍 idempotente

    # OVERWRITE (sempre substitui) — idempotente
    shutil.copy(result["model_path"], prod_dir / "model.pkl")

    # ❌ NÃO faça: append em log
    # with open("deploys.log", "a") as f:  # acumula entre runs
    #     f.write(...)
```

✅ Pipeline idempotente.

---

## 📋 Parte 6: Boas Práticas (resumo)

### Passo 12: Checklist

| Boa prática | Por quê |
|-------------|---------|
| ✅ `retries=3` em todas tasks | Tolerância a falhas transitórias |
| ✅ `execution_timeout` definido | Evita task pendurada |
| ✅ `on_failure_callback` | Alerta humano quando algo crítico falha |
| ✅ `catchup=False` | Não roda histórico do `start_date` |
| ✅ Timezone explícito | Operação clara para humanos |
| ✅ Tasks idempotentes | Permite re-execução segura |
| ✅ Tasks pequenas e focadas | Facilita debug e retry parcial |
| ✅ Sem lógica de negócio no DAG file | DAG = orquestrador, código em módulos |
| ✅ Tags na DAG | Filtrar na UI |

---

## 🔧 Troubleshooting

| Erro | Causa | Solução |
|------|-------|---------|
| Tasks rodando "todas de uma vez" | `catchup=True` (default) | Mudar para `False` |
| Alerta não dispara | Função não tem assinatura `context` | `def alert(context):` (1 argumento) |
| Retry não acontece | `retries=0` | Setar `retries=N` em `default_args` |
| Timezone errado | Padrão UTC | Usar `pendulum.timezone(...)` |
| Backoff cresce demais | Sem `max_retry_delay` | Limitar com `max_retry_delay=timedelta(hours=1)` |
| Task duplicada após retry | Não idempotente | Verificar `INSERT`/`mkdir`/etc |

---

**FIM DO VÍDEO 5.3** ✅

---

## 🏆 Recapitulação da Aula 05

| Vídeo | O que aprendeu |
|-------|---------------|
| **5.1** | Airflow setup, UI, primeira DAG |
| **5.2** | Pipeline ML end-to-end com XCom |
| **5.3** | Retry, timeout, alertas, schedule, idempotência |

**Próxima aula:** Reprodutibilidade e Qualidade do Código 📊
