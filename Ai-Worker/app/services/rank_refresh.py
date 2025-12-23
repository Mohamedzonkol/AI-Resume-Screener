from typing import List, Tuple
from app.core.db import fetchone, fetchall, execute
from app.services.embeddings import embed_text, EMBED_MODEL_DEFAULT
from app.services.openai_client import get_openai

UPSERT_JOB_EMBED_SQL = """
                       INSERT INTO embeddings (embedding_id, owner_type, owner_id, model, vector)
                       VALUES (gen_random_uuid(), 'job', :owner_id, :model, :vector)
                           ON CONFLICT (owner_type, owner_id) DO UPDATE
                                                                     SET model = EXCLUDED.model,
                                                                     vector = EXCLUDED.vector; \
                       """

READ_JOB_SQL = "SELECT title, description FROM jobs WHERE job_id = :job_id"

SIMILARITY_SQL = """
                 SELECT
                     c.candidate_id,
                     c.full_name,
                     c.email,
                     c.resume_text,
                     (1 - (e.vector <=> CAST(:job_vec AS vector(1536)))) AS similarity
                 FROM embeddings e
                          JOIN candidates c ON c.candidate_id = e.owner_id
                 WHERE e.owner_type = 'candidate'
                 ORDER BY e.vector <=> CAST(:job_vec AS vector(1536)) ASC
LIMIT :topk \
                 """

UPSERT_RANKING_SQL = """
                     INSERT INTO rankings (ranking_id, job_id, candidate_id, score, justification)
                     VALUES (gen_random_uuid(), :job_id, :candidate_id, :score, :justification)
                         ON CONFLICT (job_id, candidate_id) DO UPDATE
                                                                   SET score = EXCLUDED.score,
                                                                   justification = EXCLUDED.justification,
                                                                   created_at = NOW(); \
                     """

PROMPT_TEMPLATE = """You are a recruiter assistant. Given this job description and a candidate resume, explain briefly why this candidate is a strong or weak match.
Be concise (3-5 sentences). Mention 2-3 specific skill or experience alignments or gaps.

JOB:
{job_title}
{job_description}

CANDIDATE:
Name: {full_name}
Email: {email}
Resume Text: {resume_text}

Your analysis:
"""

def _ensure_job_embedding(job_id: str, model: str):
    job = fetchone(READ_JOB_SQL, {"job_id": job_id})
    if not job:
        raise RuntimeError(f"Job {job_id} not found")
    text = f"{job['title']}\n\n{job['description']}".strip()
    vec = embed_text(text, model)
    execute(UPSERT_JOB_EMBED_SQL, {"owner_id": job_id, "model": model, "vector": vec})
    return vec, job["title"], job["description"]

def _justify_matches(job_title: str, job_desc: str, rows: List[dict], gpt_model: str) -> List[Tuple[str, float, str]]:
    client = get_openai()
    out: List[Tuple[str, float, str]] = []
    for r in rows:
        prompt = PROMPT_TEMPLATE.format(
            job_title=job_title,
            job_description=job_desc,
            full_name=r["full_name"] or "",
            email=r["email"] or "",
            resume_text=(r["resume_text"] or "")[:6000],
        )
        resp = client.chat.completions.create(
            model=gpt_model,
            messages=[
                {"role": "system", "content": "You are an expert technical recruiter."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=220
        )
        justification = resp.choices[0].message.content.strip()
        out.append((str(r["candidate_id"]), float(r["similarity"]), justification))
    return out

async def handle_rank_refresh(msg: dict):
    job_id = str(msg["JobId"])
    embed_model = msg.get("EmbeddingModel", EMBED_MODEL_DEFAULT)
    gpt_model = msg.get("GptModel", "gpt-4o-mini")
    top_k = int(msg.get("TopK", 25))

    job_vec, job_title, job_desc = _ensure_job_embedding(job_id, embed_model)
    rows = fetchall(SIMILARITY_SQL, {"job_vec": job_vec, "topk": top_k})
    results = _justify_matches(job_title, job_desc, rows, gpt_model)
from typing import List, Tuple
from app.core.db import fetchone, fetchall, execute
from app.services.embeddings import embed_text, EMBED_MODEL_DEFAULT
from app.services.openai_client import get_openai

UPSERT_JOB_EMBED_SQL = """
                       INSERT INTO embeddings (embedding_id, owner_type, owner_id, model, vector)
                       VALUES (gen_random_uuid(), 'job', :owner_id, :model, :vector)
                           ON CONFLICT (owner_type, owner_id) DO UPDATE
                                                                     SET model = EXCLUDED.model,
                                                                     vector = EXCLUDED.vector; \
                       """

READ_JOB_SQL = "SELECT title, description FROM jobs WHERE job_id = :job_id"

SIMILARITY_SQL = """
                 SELECT
                     c.candidate_id,
                     c.full_name,
                     c.email,
                     c.resume_text,
                     (1 - (e.vector <=> CAST(:job_vec AS vector(1536)))) AS similarity
                 FROM embeddings e
                          JOIN candidates c ON c.candidate_id = e.owner_id
                 WHERE e.owner_type = 'candidate'
                 ORDER BY e.vector <=> CAST(:job_vec AS vector(1536)) ASC
LIMIT :topk \
                 """

UPSERT_RANKING_SQL = """
                     INSERT INTO rankings (ranking_id, job_id, candidate_id, score, justification)
                     VALUES (gen_random_uuid(), :job_id, :candidate_id, :score, :justification)
                         ON CONFLICT (job_id, candidate_id) DO UPDATE
                                                                   SET score = EXCLUDED.score,
                                                                   justification = EXCLUDED.justification,
                                                                   created_at = NOW(); \
                     """

PROMPT_TEMPLATE = """You are a recruiter assistant. Given this job description and a candidate resume, explain briefly why this candidate is a strong or weak match.
Be concise (3-5 sentences). Mention 2-3 specific skill or experience alignments or gaps.

JOB:
{job_title}
{job_description}

CANDIDATE:
Name: {full_name}
Email: {email}
Resume Text: {resume_text}

Your analysis:
"""

def _ensure_job_embedding(job_id: str, model: str):
    job = fetchone(READ_JOB_SQL, {"job_id": job_id})
    if not job:
        raise RuntimeError(f"Job {job_id} not found")
    text = f"{job['title']}\n\n{job['description']}".strip()
    vec = embed_text(text, model)
    execute(UPSERT_JOB_EMBED_SQL, {"owner_id": job_id, "model": model, "vector": vec})
    return vec, job["title"], job["description"]

def _justify_matches(job_title: str, job_desc: str, rows: List[dict], gpt_model: str) -> List[Tuple[str, float, str]]:
    client = get_openai()
    out: List[Tuple[str, float, str]] = []
    for r in rows:
        prompt = PROMPT_TEMPLATE.format(
            job_title=job_title,
            job_description=job_desc,
            full_name=r["full_name"] or "",
            email=r["email"] or "",
            resume_text=(r["resume_text"] or "")[:6000],
        )
        resp = client.chat.completions.create(
            model=gpt_model,
            messages=[
                {"role": "system", "content": "You are an expert technical recruiter."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
            max_tokens=220
        )
        justification = resp.choices[0].message.content.strip()
        out.append((str(r["candidate_id"]), float(r["similarity"]), justification))
    return out

async def handle_rank_refresh(msg: dict):
    job_id = str(msg["JobId"])
    embed_model = msg.get("EmbeddingModel", EMBED_MODEL_DEFAULT)
    gpt_model = msg.get("GptModel", "gpt-4o-mini")
    top_k = int(msg.get("TopK", 25))

    job_vec, job_title, job_desc = _ensure_job_embedding(job_id, embed_model)
    rows = fetchall(SIMILARITY_SQL, {"job_vec": job_vec, "topk": top_k})
    results = _justify_matches(job_title, job_desc, rows, gpt_model)

    for candidate_id, score, justification in results:
        execute(UPSERT_RANKING_SQL, {
            "job_id": job_id,
            "candidate_id": candidate_id,
            "score": score,
            "justification": justification
        })

    print(f"[AI Worker] Rankings refreshed for job {job_id}: {len(results)} candidates", flush=True)
    for candidate_id, score, justification in results:
        execute(UPSERT_RANKING_SQL, {
            "job_id": job_id,
            "candidate_id": candidate_id,
            "score": score,
            "justification": justification
        })

    print(f"[AI Worker] Rankings refreshed for job {job_id}: {len(results)} candidates", flush=True)