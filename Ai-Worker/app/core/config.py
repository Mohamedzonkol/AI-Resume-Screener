import os
from pydantic import BaseModel

class Settings(BaseModel):
    DATABASE_URL: str| None = os.getenv("DATABASE_URL")
    RABBITMQ_URL: str| None = os.getenv("RABBITMQ_URL")
    OPENAI_API_KEY: str| None = os.getenv("OPENAI_API_KEY")
    WEB_ORIGIN: str| None = os.getenv("WEB_ORIGIN")

settings = Settings()