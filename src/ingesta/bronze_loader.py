import os
from datetime import datetime

import pandas as pd

from src.utils.config import Config
from src.utils.logger import get_logger
from src.ingesta.generate_data import (
    generate_users,
    generate_transactions,
    generate_transaction_details,
)

logger = get_logger("ingesta.bronze_loader")


def save_to_bronze(df: pd.DataFrame, entity: str) -> str:
    """Guarda un DataFrame en la capa Bronze en formato Parquet con particion por fecha."""
    date_partition = datetime.now().strftime("%Y-%m-%d")
    directory = os.path.join(Config.BRONZE_PATH, entity, f"date={date_partition}")
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{entity}.parquet")
    df.to_parquet(path, index=False)
    logger.info(f"💾 Guardado: {path} ({len(df)} registros)")
    return path


def already_processed(entity: str) -> bool:
    """Verifica si ya existe data para el dia de hoy."""
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(
        Config.BRONZE_PATH, entity,
        f"date={date_partition}", f"{entity}.parquet"
    )
    return os.path.exists(path)


def run_bronze(force: bool = False) -> dict:
    """Ejecuta la ingesta completa hacia Bronze con procesamiento incremental."""
    logger.info("=" * 50)
    logger.info("INICIANDO INGESTA — CAPA BRONZE")
    logger.info("=" * 50)

    start = datetime.now()

    # Procesamiento incremental — no reprocesar si ya existe
    if not force:
        already_done = all([
            already_processed("users"),
            already_processed("transactions"),
            already_processed("transaction_details")
        ])
        if already_done:
            logger.info("⏭️  INCREMENTAL: Data de hoy ya existe en Bronze — omitiendo ingesta")
            logger.info("   Usa force=True para forzar reprocesamiento")
            date_partition = datetime.now().strftime("%Y-%m-%d")
            return {
                "paths": {
                    "users":               f"{Config.BRONZE_PATH}/users/date={date_partition}/users.parquet",
                    "transactions":        f"{Config.BRONZE_PATH}/transactions/date={date_partition}/transactions.parquet",
                    "transaction_details": f"{Config.BRONZE_PATH}/transaction_details/date={date_partition}/transaction_details.parquet"
                },
                "counts": {"skipped": True},
                "incremental": True
            }

    # Generar datos
    users_df        = generate_users(n=100)
    transactions_df = generate_transactions(users_df, n=500)
    details_df      = generate_transaction_details(transactions_df)

    # Guardar en Bronze
    paths = {
        "users":               save_to_bronze(users_df, "users"),
        "transactions":        save_to_bronze(transactions_df, "transactions"),
        "transaction_details": save_to_bronze(details_df, "transaction_details")
    }

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"✅ BRONZE COMPLETO en {elapsed:.2f}s")
    logger.info(f"   Users:        {len(users_df)} registros")
    logger.info(f"   Transactions: {len(transactions_df)} registros")
    logger.info(f"   Details:      {len(details_df)} registros")

    return {
        "paths": paths,
        "counts": {
            "users":               len(users_df),
            "transactions":        len(transactions_df),
            "transaction_details": len(details_df)
        },
        "incremental": False
    }