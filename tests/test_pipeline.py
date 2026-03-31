import pytest
import pandas as pd
from src.ingesta.generate_data import generate_users, generate_transactions


def test_generate_users():
    df = generate_users(n=10)
    assert len(df) == 10
    assert "user_id" in df.columns
    assert "email" in df.columns
    assert df["user_id"].nunique() == 10


def test_generate_transactions():
    users = generate_users(n=10)
    df = generate_transactions(users, n=20)
    assert len(df) >= 20
    assert "transaction_id" in df.columns
    assert "amount" in df.columns
    assert "status" in df.columns


def test_amount_positive_rule():
    users = generate_users(n=10)
    df = generate_transactions(users, n=50)
    df_clean = df[df["amount"] > 0]
    assert (df_clean["amount"] > 0).all()


def test_status_values():
    users = generate_users(n=10)
    df = generate_transactions(users, n=50)
    valid = {"success", "failed", "SUCCESS", "Failed", "FAILED"}
    assert df["status"].isin(valid).all()

    