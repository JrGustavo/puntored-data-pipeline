import os
import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.config import Config
from src.ingesta.generate_data import (
    generate_users,
    generate_transactions,
    generate_transaction_details
)

logger = get_logger("ingesta.bronze_loader")


def save_to_bronze(df: pd.DataFrame, entity: str) -> str:
    """Guarda datos crudos en Bronze particionado por fecha."""
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(Config.BRONZE_PATH, entity, f"date={date_partition}")
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, f"{entity}.parquet")
    df.to_parquet(file_path, index=False)

    logger.info(f"✅ Bronze guardado: {file_path} ({len(df)} registros)")
    return file_path


def run_bronze() -> dict:
    """Ejecuta la ingesta completa hacia Bronze."""
    logger.info("=" * 50)
    logger.info("INICIANDO INGESTA — CAPA BRONZE")
    logger.info("=" * 50)

    start = datetime.now()

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
        }
    }


if __name__ == "__main__":
    run_bronze()