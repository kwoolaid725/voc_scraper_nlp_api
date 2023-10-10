# from pydantic import BaseSettings
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    base_dir: Path = Path(__file__).resolve().parent
    templates_dir: Path = Path(__file__).resolve().parent / "templates"
    database_hostname: str
    database_port: str
    database_password: str
    database_name: str
    database_username: str
    # secret_key: str
    # algorithm: str
    # access_token_expire_minutes: int

    class Config:
        env_file = ".env"


settings = Settings()


