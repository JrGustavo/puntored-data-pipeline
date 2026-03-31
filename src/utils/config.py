import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
    BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
    SILVER_PATH = os.getenv("SILVER_PATH", "data/silver")
    GOLD_PATH   = os.getenv("GOLD_PATH", "data/gold")
    LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_paths(cls):
        return {
            "bronze": cls.BRONZE_PATH,
            "silver": cls.SILVER_PATH,
            "gold":   cls.GOLD_PATH
        }