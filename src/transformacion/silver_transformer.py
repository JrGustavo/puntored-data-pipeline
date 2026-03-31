import os
import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.config import Config

logger = get_logger("transformacion.silver")


def load_bronze(entity: str) -> pd.DataFrame:
    """Carga datos desde Bronze."""
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(
        Config.BRONZE_PATH, entity,
        f"date={date_partition}", f"{entity}.parquet"
    )
    df = pd.read_parquet(path)
    logger.info(f"Bronze cargado: {entity} ({len(df)} registros)")
    return df


def save_to_silver(df: pd.DataFrame, entity: str) -> str:
    """Guarda datos limpios en Silver."""
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(Config.SILVER_PATH, entity, f"date={date_partition}")
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, f"{entity}.parquet")
    df.to_parquet(file_path, index=False)
    logger.info(f"✅ Silver guardado: {file_path} ({len(df)} registros)")
    return file_path


def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Limpiando users...")
    before = len(df)

    # Eliminar duplicados
    df = df.drop_duplicates(subset=["user_id"])

    # Validar campos obligatorios
    df = df.dropna(subset=["user_id", "name", "email"])

    # Estandarizar email a lowercase
    df["email"] = df["email"].str.lower().str.strip()

    # Agregar metadata de trazabilidad
    df["_silver_processed_at"] = datetime.now().isoformat()
    df["_source"] = "bronze"

    logger.info(f"✅ Users: {before} → {len(df)} registros (+{before - len(df)} eliminados)")
    return df


def clean_transactions(df: pd.DataFrame, users_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Limpiando transactions...")
    before = len(df)

    # Eliminar duplicados por transaction_id
    df = df.drop_duplicates(subset=["transaction_id"])
    logger.info(f"  Duplicados eliminados: {before - len(df)}")

    # Regla de negocio: amount > 0
    invalid_amount = df[df["amount"] <= 0]
    if len(invalid_amount) > 0:
        logger.warning(f"  ⚠️ {len(invalid_amount)} transacciones con amount <= 0 eliminadas")
    df = df[df["amount"] > 0]

    # Estandarizar status a lowercase
    df["status"] = df["status"].str.lower().str.strip()

    # Validar status permitidos
    valid_statuses = {"success", "failed"}
    invalid_status = df[~df["status"].isin(valid_statuses)]
    if len(invalid_status) > 0:
        logger.warning(f"  ⚠️ {len(invalid_status)} registros con status invalido eliminados")
    df = df[df["status"].isin(valid_statuses)]

    # Integridad referencial: solo transacciones con usuario valido
    valid_users = set(users_df["user_id"])
    orphan_tx = df[~df["user_id"].isin(valid_users)]
    if len(orphan_tx) > 0:
        logger.warning(f"  ⚠️ {len(orphan_tx)} transacciones sin usuario valido eliminadas")
    df = df[df["user_id"].isin(valid_users)]

    # Convertir created_at a datetime
    df["created_at"] = pd.to_datetime(df["created_at"])

    # Agregar metadata de trazabilidad
    df["_silver_processed_at"] = datetime.now().isoformat()
    df["_source"] = "bronze"

    logger.info(f"✅ Transactions: {before} → {len(df)} registros limpios")
    return df


def clean_transaction_details(df: pd.DataFrame, transactions_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Limpiando transaction_details...")
    before = len(df)

    # Eliminar duplicados
    df = df.drop_duplicates(subset=["detail_id"])

    # Integridad referencial: solo detalles con transaccion valida
    valid_tx = set(transactions_df["transaction_id"])
    orphan_details = df[~df["transaction_id"].isin(valid_tx)]
    if len(orphan_details) > 0:
        logger.warning(f"  ⚠️ {len(orphan_details)} detalles sin transaccion valida eliminados")
    df = df[df["transaction_id"].isin(valid_tx)]

    # Validar payment_method
    valid_methods = {"card", "pse", "cash", "nequi", "daviplata"}
    df["payment_method"] = df["payment_method"].str.lower().str.strip()
    df = df[df["payment_method"].isin(valid_methods)]

    # Validar channel
    valid_channels = {"web", "mobile", "api"}
    df["channel"] = df["channel"].str.lower().str.strip()
    df = df[df["channel"].isin(valid_channels)]

    # Validar processing_time_ms positivo
    df = df[df["processing_time_ms"] > 0]

    # Manejo de nulls en campos no obligatorios
    df["payment_method"] = df["payment_method"].fillna("unknown")
    df["channel"] = df["channel"].fillna("unknown")

    # Agregar metadata
    df["_silver_processed_at"] = datetime.now().isoformat()
    df["_source"] = "bronze"

    logger.info(f"✅ Details: {before} → {len(df)} registros limpios")
    return df


def run_silver() -> dict:
    """Ejecuta transformacion completa Bronze → Silver."""
    logger.info("=" * 50)
    logger.info("INICIANDO TRANSFORMACION — CAPA SILVER")
    logger.info("=" * 50)

    start = datetime.now()

    # Cargar Bronze
    users_raw        = load_bronze("users")
    transactions_raw = load_bronze("transactions")
    details_raw      = load_bronze("transaction_details")

    # Limpiar en orden — users primero para integridad referencial
    users_clean        = clean_users(users_raw)
    transactions_clean = clean_transactions(transactions_raw, users_clean)
    details_clean      = clean_transaction_details(details_raw, transactions_clean)

    # Guardar en Silver
    paths = {
        "users":               save_to_silver(users_clean, "users"),
        "transactions":        save_to_silver(transactions_clean, "transactions"),
        "transaction_details": save_to_silver(details_clean, "transaction_details")
    }

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"✅ SILVER COMPLETO en {elapsed:.2f}s")
    logger.info(f"   Users:        {len(users_clean)} registros limpios")
    logger.info(f"   Transactions: {len(transactions_clean)} registros limpios")
    logger.info(f"   Details:      {len(details_clean)} registros limpios")

    return {
        "paths": paths,
        "counts": {
            "users":               len(users_clean),
            "transactions":        len(transactions_clean),
            "transaction_details": len(details_clean)
        }
    }


if __name__ == "__main__":
    run_silver()