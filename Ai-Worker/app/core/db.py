from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from app.core.config import settings
from pgvector.psycopg import register_vector  

_engine: Engine | None = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        sa_url = settings.DATABASE_URL
        if not sa_url or "postgres" not in sa_url:
            raise RuntimeError(f"Bad or missing DATABASE_URL: {sa_url!r}")
        eng = create_engine(sa_url, pool_pre_ping=True, future=True)

        @event.listens_for(eng, "connect")
        def _on_connect(dbapi_conn, _):
            try:
                register_vector(dbapi_conn)
            except Exception as e:
                print(f"[AI Worker] register_vector warning: {e}", flush=True)

        _engine = eng
    return _engine

def execute(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.begin() as conn:
        return conn.execute(text(sql), params or {})

def fetchall(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text(sql), params or {}).mappings().all()

def fetchone(sql: str, params: dict | None = None):
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text(sql), params or {}).mappings().first()