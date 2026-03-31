import os
import duckdb
import pandas as pd
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.config import Config

logger = get_logger("modelado.gold")


def load_silver(entity: str) -> pd.DataFrame:
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(
        Config.SILVER_PATH, entity,
        f"date={date_partition}", f"{entity}.parquet"
    )
    df = pd.read_parquet(path)
    logger.info(f"Silver cargado: {entity} ({len(df)} registros)")
    return df


def save_to_gold(df: pd.DataFrame, name: str) -> str:
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(Config.GOLD_PATH, f"date={date_partition}")
    os.makedirs(path, exist_ok=True)
    file_path = os.path.join(path, f"{name}.parquet")
    df.to_parquet(file_path, index=False)
    logger.info(f"✅ Gold guardado: {file_path} ({len(df)} registros)")
    return file_path


def build_gold(con: duckdb.DuckDBPyConnection) -> dict:
    paths = {}

    # ── KPIs por usuario ──────────────────────────────────────
    logger.info("Construyendo gold_user_metrics...")
    df = con.execute("""
        SELECT
            u.user_id,
            u.name,
            u.email,
            COUNT(t.transaction_id)                                    AS total_transactions,
            SUM(t.amount)                                              AS total_amount,
            AVG(t.amount)                                              AS avg_amount,
            MAX(t.amount)                                              AS max_amount,
            MIN(t.amount)                                              AS min_amount,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END)          AS successful_transactions,
            COUNT(CASE WHEN t.status = 'failed'  THEN 1 END)          AS failed_transactions,
            ROUND(
                COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0
                / NULLIF(COUNT(t.transaction_id), 0), 2
            )                                                          AS success_rate_pct,
            MIN(t.created_at)                                          AS first_transaction_at,
            MAX(t.created_at)                                          AS last_transaction_at
        FROM users u
        LEFT JOIN transactions t ON u.user_id = t.user_id
        GROUP BY u.user_id, u.name, u.email
        ORDER BY total_amount DESC NULLS LAST
    """).df()
    paths["gold_user_metrics"] = save_to_gold(df, "gold_user_metrics")

    # ── KPIs por canal ────────────────────────────────────────
    logger.info("Construyendo gold_channel_metrics...")
    df = con.execute("""
        SELECT
            td.channel,
            COUNT(DISTINCT t.transaction_id)              AS total_transactions,
            SUM(t.amount)                                 AS total_amount,
            AVG(t.amount)                                 AS avg_amount,
            ROUND(AVG(td.processing_time_ms), 2)          AS avg_processing_time_ms,
            COUNT(CASE WHEN t.status = 'success' THEN 1 END) AS successful,
            COUNT(CASE WHEN t.status = 'failed'  THEN 1 END) AS failed,
            ROUND(
                COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0
                / NULLIF(COUNT(DISTINCT t.transaction_id), 0), 2
            )                                             AS success_rate_pct
        FROM transactions t
        JOIN transaction_details td ON t.transaction_id = td.transaction_id
        GROUP BY td.channel
        ORDER BY total_amount DESC
    """).df()
    paths["gold_channel_metrics"] = save_to_gold(df, "gold_channel_metrics")

    # ── KPIs por metodo de pago ───────────────────────────────
    logger.info("Construyendo gold_payment_method_metrics...")
    df = con.execute("""
        SELECT
            td.payment_method,
            COUNT(DISTINCT t.transaction_id)              AS total_transactions,
            SUM(t.amount)                                 AS total_amount,
            AVG(t.amount)                                 AS avg_amount,
            ROUND(AVG(td.processing_time_ms), 2)          AS avg_processing_time_ms,
            ROUND(
                COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0
                / NULLIF(COUNT(DISTINCT t.transaction_id), 0), 2
            )                                             AS success_rate_pct
        FROM transactions t
        JOIN transaction_details td ON t.transaction_id = td.transaction_id
        GROUP BY td.payment_method
        ORDER BY total_transactions DESC
    """).df()
    paths["gold_payment_method_metrics"] = save_to_gold(df, "gold_payment_method_metrics")

    # ── KPIs generales del negocio ────────────────────────────
    logger.info("Construyendo gold_summary...")
    df = con.execute("""
        SELECT
            COUNT(DISTINCT t.transaction_id)                           AS total_transactions,
            COUNT(DISTINCT t.user_id)                                  AS active_users,
            SUM(t.amount)                                              AS total_volume,
            AVG(t.amount)                                              AS avg_transaction_amount,
            ROUND(
                COUNT(CASE WHEN t.status = 'success' THEN 1 END) * 100.0
                / NULLIF(COUNT(t.transaction_id), 0), 2
            )                                                          AS overall_success_rate_pct,
            ROUND(AVG(td.processing_time_ms), 2)                       AS avg_processing_time_ms,
            MIN(t.created_at)                                          AS pipeline_start,
            MAX(t.created_at)                                          AS pipeline_end,
            CURRENT_TIMESTAMP                                          AS generated_at
        FROM transactions t
        LEFT JOIN transaction_details td ON t.transaction_id = td.transaction_id
    """).df()
    paths["gold_summary"] = save_to_gold(df, "gold_summary")

    return paths


def run_gold() -> dict:
    logger.info("=" * 50)
    logger.info("INICIANDO MODELADO — CAPA GOLD")
    logger.info("=" * 50)

    start = datetime.now()

    # Cargar Silver
    users_df   = load_silver("users")
    tx_df      = load_silver("transactions")
    details_df = load_silver("transaction_details")

    # Registrar en DuckDB en memoria
    con = duckdb.connect()
    con.register("users",               users_df)
    con.register("transactions",        tx_df)
    con.register("transaction_details", details_df)

    # Construir Gold
    paths = build_gold(con)
    con.close()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"✅ GOLD COMPLETO en {elapsed:.2f}s")
    for name, path in paths.items():
        logger.info(f"   {name}: {path}")

    return {"paths": paths}


if __name__ == "__main__":
    run_gold()