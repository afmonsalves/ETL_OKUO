# src/ETL_OKUO/config.py

from typing import Optional
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings, loaded from a .env file."""
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: Optional[str] = "us-west-2"
    s3_bucket: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def get_settings() -> Settings:
    """
    Read settings from the environment (or .env) and return a Settings instance.
    """
    return Settings()
