from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
from app.core.config import settings
from app.services.rabbit_consumer import start_consumers

app = FastAPI(title="AI Worker")

allowed_origins = [settings.WEB_ORIGIN or "http://localhost:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    print("[AI Worker] FastAPI startup: launching RabbitMQ consumer thread…", flush=True)
    print(f"[AI Worker] RABBITMQ_URL present? {'yes' if settings.RABBITMQ_URL else 'no'}", flush=True)
    t = threading.Thread(target=start_consumers, name="rabbit-consumer", daemon=True)
    t.start()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "rabbit_env": bool(settings.RABBITMQ_URL),
        "db_env": bool(settings.DATABASE_URL),
    }