import pandas as pd
import uuid
import random
from faker import Faker
from datetime import datetime, timedelta
from src.utils.logger import get_logger

fake = Faker("es_CO")
logger = get_logger("ingesta.generate_data")

def generate_users(n: int = 100) -> pd.DataFrame:
    logger.info(f"Generando {n} usuarios...")
    users = []
    for _ in range(n):
        users.append({
            "user_id":    str(uuid.uuid4()),
            "name":       fake.name(),
            "email":      fake.email(),
            "created_at": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat()
        })
    df = pd.DataFrame(users)
    logger.info(f"✅ {len(df)} usuarios generados")
    return df


def generate_transactions(users_df: pd.DataFrame, n: int = 500) -> pd.DataFrame:
    logger.info(f"Generando {n} transacciones...")
    user_ids = users_df["user_id"].tolist()
    statuses = ["success", "failed", "SUCCESS", "Failed", "FAILED"]  # intencional — para limpiar en Silver
    transactions = []

    for _ in range(n):
        transactions.append({
            "transaction_id": str(uuid.uuid4()),
            "user_id":        random.choice(user_ids),
            "amount":         round(random.uniform(-500, 980000), 2),  # algunos negativos — para validar
            "status":         random.choice(statuses),
            "created_at":     fake.date_time_between(
                start_date="-1y", end_date="now"
            ).isoformat()
        })

    # Agregar algunos duplicados intencionales para probar limpieza
    duplicates = random.sample(transactions, 10)
    transactions.extend(duplicates)

    df = pd.DataFrame(transactions)
    logger.info(f"✅ {len(df)} transacciones generadas (con duplicados intencionales)")
    return df


def generate_transaction_details(transactions_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Generando detalles de transacciones...")
    payment_methods = ["card", "pse", "cash", "nequi", "daviplata"]
    channels        = ["web", "mobile", "api"]
    details = []

    for _, tx in transactions_df.iterrows():
        # Algunas transacciones tienen 2 detalles
        num_details = random.choices([1, 2], weights=[0.8, 0.2])[0]
        for _ in range(num_details):
            details.append({
                "detail_id":          str(uuid.uuid4()),
                "transaction_id":     tx["transaction_id"],
                "payment_method":     random.choice(payment_methods),
                "channel":            random.choice(channels),
                "processing_time_ms": random.randint(50, 3000)
            })

    df = pd.DataFrame(details)
    logger.info(f"✅ {len(df)} detalles generados")
    return df