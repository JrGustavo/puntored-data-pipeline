import os
import pandas as pd
from datetime import datetime, timedelta
from src.utils.logger import get_logger
from src.utils.config import Config

logger = get_logger("calidad.quality_checks")


class QualityReport:
    def __init__(self):
        self.checks = []
        self.passed = 0
        self.failed = 0

    def add(self, entity: str, check: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.checks.append({
            "entity":  entity,
            "check":   check,
            "status":  status,
            "detail":  detail,
            "checked_at": datetime.now().isoformat()
        })
        if passed:
            self.passed += 1
        else:
            self.failed += 1
        icon = "✅" if passed else "❌"
        logger.info(f"  {icon} [{entity}] {check}: {detail}")

    def summary(self) -> dict:
        total = self.passed + self.failed
        return {
            "total_checks": total,
            "passed":       self.passed,
            "failed":       self.failed,
            "pass_rate":    round(self.passed * 100 / total, 1) if total > 0 else 0
        }

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.checks)


def load_silver(entity: str) -> pd.DataFrame:
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(
        Config.SILVER_PATH, entity,
        f"date={date_partition}", f"{entity}.parquet"
    )
    return pd.read_parquet(path)


def load_bronze(entity: str) -> pd.DataFrame:
    date_partition = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(
        Config.BRONZE_PATH, entity,
        f"date={date_partition}", f"{entity}.parquet"
    )
    return pd.read_parquet(path)


def run_quality_checks() -> QualityReport:
    logger.info("=" * 50)
    logger.info("INICIANDO CALIDAD Y OBSERVABILIDAD — NIVEL 3")
    logger.info("=" * 50)

    report = QualityReport()

    # ── Cargar datos ──────────────────────────────
    users_b   = load_bronze("users")
    tx_b      = load_bronze("transactions")
    details_b = load_bronze("transaction_details")

    users_s   = load_silver("users")
    tx_s      = load_silver("transactions")
    details_s = load_silver("transaction_details")

    # ── 1. Conteo de registros ────────────────────
    logger.info("Ejecutando: conteo de registros...")
    report.add("users",        "record_count",
        len(users_s) > 0,
        f"{len(users_s)} registros en Silver")

    report.add("transactions", "record_count",
        len(tx_s) > 0,
        f"{len(tx_s)} registros en Silver")

    report.add("transaction_details", "record_count",
        len(details_s) > 0,
        f"{len(details_s)} registros en Silver")

    # ── 2. Trazabilidad Bronze → Silver ───────────
    logger.info("Ejecutando: trazabilidad Bronze → Silver...")
    report.add("transactions", "bronze_to_silver_traceability",
        len(tx_s) <= len(tx_b),
        f"Bronze={len(tx_b)} → Silver={len(tx_s)} (se eliminaron {len(tx_b) - len(tx_s)} invalidos)")

    report.add("transaction_details", "bronze_to_silver_traceability",
        len(details_s) <= len(details_b),
        f"Bronze={len(details_b)} → Silver={len(details_s)}")

    # ── 3. Null checks ────────────────────────────
    logger.info("Ejecutando: null checks...")
    for col in ["user_id", "name", "email"]:
        nulls = users_s[col].isnull().sum()
        report.add("users", f"null_check_{col}",
            nulls == 0, f"{nulls} nulls en columna {col}")

    for col in ["transaction_id", "user_id", "amount", "status"]:
        nulls = tx_s[col].isnull().sum()
        report.add("transactions", f"null_check_{col}",
            nulls == 0, f"{nulls} nulls en columna {col}")

    # ── 4. Reglas de negocio ──────────────────────
    logger.info("Ejecutando: reglas de negocio...")

    # Amount > 0
    invalid_amount = (tx_s["amount"] <= 0).sum()
    report.add("transactions", "amount_positive",
        invalid_amount == 0,
        f"{invalid_amount} transacciones con amount <= 0")

    # Status estandarizado
    valid_statuses = {"success", "failed"}
    invalid_status = (~tx_s["status"].isin(valid_statuses)).sum()
    report.add("transactions", "status_standardized",
        invalid_status == 0,
        f"{invalid_status} registros con status invalido")

    # Sin duplicados
    dupes = tx_s["transaction_id"].duplicated().sum()
    report.add("transactions", "no_duplicates",
        dupes == 0,
        f"{dupes} duplicados encontrados")

    # ── 5. Integridad referencial ─────────────────
    logger.info("Ejecutando: integridad referencial...")
    valid_users = set(users_s["user_id"])
    orphan_tx   = (~tx_s["user_id"].isin(valid_users)).sum()
    report.add("transactions", "referential_integrity_users",
        orphan_tx == 0,
        f"{orphan_tx} transacciones sin usuario valido")

    valid_tx       = set(tx_s["transaction_id"])
    orphan_details = (~details_s["transaction_id"].isin(valid_tx)).sum()
    report.add("transaction_details", "referential_integrity_transactions",
        orphan_details == 0,
        f"{orphan_details} detalles sin transaccion valida")

    # ── 6. Freshness ──────────────────────────────
    logger.info("Ejecutando: freshness check...")
    tx_s["created_at"] = pd.to_datetime(tx_s["created_at"])
    latest_tx   = tx_s["created_at"].max()
    threshold   = datetime.now() - timedelta(days=365)
    is_fresh    = latest_tx > threshold
    report.add("transactions", "freshness",
        is_fresh,
        f"Transaccion mas reciente: {latest_tx}")

    # ── Resumen final ─────────────────────────────
    summary = report.summary()
    logger.info("=" * 50)
    logger.info("RESUMEN DE CALIDAD")
    logger.info(f"  Total checks : {summary['total_checks']}")
    logger.info(f"  Passed       : {summary['passed']} ✅")
    logger.info(f"  Failed       : {summary['failed']} ❌")
    logger.info(f"  Pass rate    : {summary['pass_rate']}%")
    logger.info("=" * 50)

    # Guardar reporte
    report_df = report.to_dataframe()
    report_path = os.path.join("logs", f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    report_df.to_csv(report_path, index=False)
    logger.info(f"✅ Reporte guardado: {report_path}")

    return report