from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    pricecharting_api_key: str = ""
    ebay_app_id: str = ""
    ebay_cert_id: str = ""
    ebay_oauth_token: str = ""
    tcgplayer_api_key: str = ""
    database_path: str = "./data/pokemon.db"
    log_level: str = "INFO"
    pipeline_dry_run: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
DB_PATH = Path(settings.database_path)
