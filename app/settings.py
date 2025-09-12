# settings.py
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()


class Settings(BaseSettings):
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", 6379))
    binance_api_key: str = os.getenv("BINANCE_API_KEY")
    binance_secret_key: str = os.getenv("BINANCE_SECRET_KEY")
    telegram_token: str = os.getenv("TELEGRAM_TOKEN")
    admin_id: int = int(os.getenv("ADMIN_ID", 0))


settings = Settings()
