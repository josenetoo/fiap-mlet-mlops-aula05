# Aula 05 - Orquestração com Apache Airflow

> **Repositório**: https://github.com/josenetoo/fiap-mlet-mlops-aula05

## 🎯 Objetivo

Subir Apache Airflow via Docker Compose, transformar o pipeline de ML em uma **DAG** e aplicar boas práticas de produção: **retry**, **timeout**, **alertas**, **schedule** e **idempotência**.

## 📹 Vídeos desta Aula

| Vídeo | Tema | O que você vai fazer |
|-------|------|---------------------|
| 01 | Introdução ao Apache Airflow | Subir Airflow, primeira DAG, navegar na UI |
| 02 | Pipeline de ML com Airflow | DAG `ml_pipeline` end-to-end com XCom |
| 03 | Monitoramento e Boas Práticas | Retry, timeout, alertas, schedule, idempotência |

## 🏗️ Pipeline desta Aula

```
DAG: ml_pipeline

ingest_data → validate_data → train_model → evaluate_model → deploy_model
   ↓               ↓               ↓                ↓               ↓
 retry=3      threshold       sklearn fit    accuracy>=0.9    copia .pkl
```

Detalhes em [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## 📁 Estrutura do Repositório

```
.
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yaml        # Airflow oficial (baixado no hands-on)
├── docs/
│   ├── ARCHITECTURE.md
│   ├── CHEATSHEET.md
│   ├── HANDS-ON-05-01.md      # Setup
│   ├── HANDS-ON-05-02.md      # Pipeline ML
│   └── HANDS-ON-05-03.md      # Boas práticas
├── dags/
│   ├── hello_world_dag.py
│   ├── ml_pipeline_dag.py
│   └── ml_pipeline/
│       └── tasks/
│           └── ml_tasks.py
├── logs/                      # Gerado pelo Airflow (gitignored)
└── plugins/
```

## Pré-requisitos

| Requisito | Como verificar |
|-----------|----------------|
| Aula 04 concluída | Docker rodando |
| Docker Desktop | `docker --version` |
| Docker Compose v2 | `docker compose version` |
| 4 GB RAM livres | Airflow puxa ~3 GB |
| Porta 8080 livre | `lsof -i :8080` |

> ⚠️ Airflow é mais "pesado" que outras ferramentas — aloque RAM no Docker Desktop antes.

## 🚀 Como Usar

1. **Fork** e clone este repositório
2. Siga os hands-on em `docs/HANDS-ON-05-*.md`
3. Hands-on 5.1 mostra como baixar o `docker-compose.yaml` oficial
4. UI do Airflow: http://localhost:8080 (login: `airflow` / `airflow`)

## 📚 Documentação

| Vídeo | Hands-on |
|-------|----------|
| 01 - Introdução ao Apache Airflow | [HANDS-ON-05-01.md](docs/HANDS-ON-05-01.md) |
| 02 - Pipeline de ML com Airflow | [HANDS-ON-05-02.md](docs/HANDS-ON-05-02.md) |
| 03 - Monitoramento e Boas Práticas | [HANDS-ON-05-03.md](docs/HANDS-ON-05-03.md) |

**Referência rápida**: [Cheatsheet](docs/CHEATSHEET.md)

---

**FIAP - Pós Tech Machine Learning Engineering**
