import os

from pydantic import BaseSettings


class Settings(BaseSettings):
    """It contains the parameters used by the backend.
    They are then explicitly adopted by the single modules.

    Args:
        BaseSettings: `pydantic` class
    """

    app_name: str = "MODERATE"
    app_description: str = "MODERATE Access Point for Quality Report"
    version: str = "1.0.0"
    crt: str = "config/localhost.crt"
    key: str = "config/localhost.key"
    config_path: str = "config/frontend.json"
    broker = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    security_protocol: str = "SASL_SSL"
    mechanism: str = "PLAIN"
    sasl_username: str = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: str = os.getenv("KAFKA_SASL_PASSWORD")
    validation_topic: str = os.getenv("VALIDATION_TOPIC") or "validation"
    kafka_server_certificate_location: str = os.getenv(
        "KAFKA_SERVER_CERTIFICATE_LOCATION"
    )
    host: str = "localhost"
    port: int = 8000
    api_keys: str = os.getenv("API_KEYS") or "ABC12345"
    response_404: dict = {"description": "Not found"}
    database: str = os.getenv("DATABASE_PATH") or "sqlite:///./report.db"
    kafka_consumer_group_id: str = (
        os.getenv("KAFKA_CONSUMER_GROUP_ID") or "quality-reporter"
    )
    kafka_auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET") or "latest"
    kafka_poll_timeout_ms: int = int(os.getenv("KAFKA_POLL_TIMEOUT_MS") or "250")
    kafka_max_poll_records: int = int(os.getenv("KAFKA_MAX_POLL_RECORDS") or "1000")
    batch_size: int = int(os.getenv("KAFKA_BATCH_SIZE") or "1000")
    batch_timeout: float = float(os.getenv("KAFKA_BATCH_TIMEOUT") or "0.25")
    healthy_after_seconds: float = float(
        os.getenv("REPORTER_HEALTHY_AFTER_SECONDS") or "5"
    )
    stale_after_seconds: float = float(
        os.getenv("REPORTER_STALE_AFTER_SECONDS") or "30"
    )


settings = Settings()

if os.getenv("CONFIG_LOCATION") is not None:
    import configparser

    config = configparser.RawConfigParser()
    config.read(os.getenv("CONFIG_LOCATION"))

    config_dict = dict(config.items("backend"))
    settings.validation_topic = config_dict["validation.topic"]
