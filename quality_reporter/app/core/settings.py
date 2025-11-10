from pydantic import BaseSettings
import os

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
    kafka_server_certificate_location: str = os.getenv("KAFKA_SERVER_CERTIFICATE_LOCATION")
    host: str = "localhost"
    port: int = 8000
    api_keys: str = os.getenv("API_KEYS") or "ABC12345"
    response_404: dict = {"description": "Not found"}
    database: str = os.getenv("DATABASE_PATH") or "sqlite:///./report.db"

settings = Settings()

if os.getenv("CONFIG_LOCATION") is not None:
    import configparser
    config = configparser.RawConfigParser()
    config.read(os.getenv("CONFIG_LOCATION"))
    
    config_dict = dict(config.items('backend'))
    settings.validation_topic = config_dict["validation.topic"]