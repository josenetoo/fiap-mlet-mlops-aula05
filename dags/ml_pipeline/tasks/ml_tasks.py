"""Tasks do pipeline de ML para Airflow."""
import json
import logging
import random
import shutil
from pathlib import Path

import joblib
import numpy as np
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

    data = np.column_stack([X, y])
    path = DATA_DIR / "raw.csv"
    np.savetxt(path, data, delimiter=",", header="sl,sw,pl,pw,target", comments="")

    logger.info(f"✅ {len(data)} linhas salvas em {path}")
    return str(path)


def validate_data(**context):
    """Valida o CSV gerado."""
    path = context["ti"].xcom_pull(task_ids="ingest_data")
    logger.info(f"🔍 Validando {path}")

    data = np.loadtxt(path, delimiter=",", skiprows=1)
    assert data.shape == (150, 5), f"Shape errado: {data.shape}"
    assert not np.isnan(data).any(), "NaN encontrado"

    logger.info(f"✅ Dados válidos: {data.shape}")
    return path


def train_model(**context):
    """Treina RandomForest."""
    path = context["ti"].xcom_pull(task_ids="validate_data")
    data = np.loadtxt(path, delimiter=",", skiprows=1)

    X, y = data[:, :4], data[:, 4].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(n_estimators=100, random_state=42).fit(X_tr, y_tr)

    model_path = DATA_DIR / "model.pkl"
    joblib.dump(model, model_path)
    logger.info(f"✅ Modelo salvo em {model_path}")

    test_path = DATA_DIR / "test.npz"
    np.savez(test_path, X_te=X_te, y_te=y_te)

    return {"model_path": str(model_path), "test_path": str(test_path)}


def evaluate_model(**context):
    """Avalia o modelo. Falha se accuracy < threshold."""
    paths = context["ti"].xcom_pull(task_ids="train_model")

    model = joblib.load(paths["model_path"])
    test = np.load(paths["test_path"])
    X_te, y_te = test["X_te"], test["y_te"]

    accuracy = float(accuracy_score(y_te, model.predict(X_te)))
    logger.info(f"📊 Accuracy: {accuracy:.3f}")

    threshold = 0.9
    if accuracy < threshold:
        raise ValueError(f"Accuracy {accuracy:.3f} < threshold {threshold}")

    return {"accuracy": accuracy, "model_path": paths["model_path"]}


def deploy_model(**context):
    """'Deploya' (copia modelo para pasta de produção)."""
    result = context["ti"].xcom_pull(task_ids="evaluate_model")

    prod_dir = DATA_DIR / "production"
    prod_dir.mkdir(parents=True, exist_ok=True)  # idempotente

    prod_path = prod_dir / "model.pkl"
    shutil.copy(result["model_path"], prod_path)

    meta = {
        "accuracy": result["accuracy"],
        "deployed_at": str(context.get("ts", "")),
    }
    (prod_dir / "metadata.json").write_text(json.dumps(meta, indent=2))

    logger.info(f"🚀 Modelo deployado em {prod_path}")
    return str(prod_path)


def flaky_task(**context):
    """Task de demonstração que falha 50% das vezes."""
    if random.random() < 0.5:
        raise RuntimeError("Falha simulada!")
    print("✅ Sucesso!")
