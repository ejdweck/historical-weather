from pydantic import ConfigDict
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env")
    
    # Database
    DATABASE_URL: str
    
    # Weather Parameters
    LATITUDE: float = 40.7128
    LONGITUDE: float = -74.0060
    TIMEZONE: str = "America/New_York"
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000

settings = Settings() 