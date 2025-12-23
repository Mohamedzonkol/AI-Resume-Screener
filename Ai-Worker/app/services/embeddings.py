from typing import List
from app.services.openai_client import get_openai

EMBED_MODEL_DEFAULT = "text-embedding-3-small"
EMBED_DIM = 1536

def embed_text(text: str, model: str = EMBED_MODEL_DEFAULT) -> List[float]:
    text = (text or "").strip()
    if not text:
        return [0.0] * EMBED_DIM
    client = get_openai()
    resp = client.embeddings.create(model=model, input=text)
    return resp.data[0].embedding