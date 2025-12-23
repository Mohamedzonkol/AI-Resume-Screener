from app.core.db import execute
from app.services.embeddings import embed_text, EMBED_MODEL_DEFAULT

UPSERT_EMBED_SQL = """
                   INSERT INTO embeddings (embedding_id, owner_type, owner_id, model, vector)
                   VALUES (gen_random_uuid(), 'candidate', :owner_id, :model, :vector)
                       ON CONFLICT (owner_type, owner_id) DO UPDATE
                                                                 SET model = EXCLUDED.model,
                                                                 vector = EXCLUDED.vector; \
                   """

async def handle_resume_parse(msg: dict):
    candidate_id = msg["CandidateId"]
    resume_text = msg.get("ResumeText") or ""
    model = msg.get("EmbeddingModel", EMBED_MODEL_DEFAULT)

    vec = embed_text(resume_text, model)

    execute(UPSERT_EMBED_SQL, {
        "owner_id": str(candidate_id),
        "model": model,
        "vector": vec
    })

    if resume_text:
        execute("""
                UPDATE candidates
                SET resume_text = :resume_text
                WHERE candidate_id = :cid
                """, {"resume_text": resume_text, "cid": str(candidate_id)})

    print(f"[AI Worker] Stored candidate embedding for {candidate_id}", flush=True)