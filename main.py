"""
FastAPI service para scrape de Google Maps via Playwright.

Endpoints:
- GET  /health               — healthcheck
- POST /scrape               — scrape síncrono (espera terminar e devolve)
- POST /scrape/async         — dispara job em background, retorna job_id
- GET  /jobs/{job_id}        — consulta status + resultado do job
"""
import asyncio
import logging
import os
import uuid
from typing import Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

from scraper import scrape_multi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

API_KEY = os.getenv("SCRAPER_API_KEY", "")

app = FastAPI(title="NG Scraper Google", version="1.0.0")


class ScrapeRequest(BaseModel):
    nicho: str = Field(..., description="Nicho / tipo de empresa")
    cidades: list[str] = Field(..., description="Lista de cidades")
    max_per_city: int = Field(500, ge=1, le=1000)
    api_key: Optional[str] = None


class Company(BaseModel):
    title: Optional[str]
    phone: Optional[str]
    website: Optional[str]
    totalScore: Optional[float]
    reviewsCount: int = 0
    imagesCount: int = 0
    address: Optional[str]


class ScrapeResponse(BaseModel):
    total: int
    companies: list[Company]
    blocked: bool = False
    reason: Optional[str] = None


def _check_auth(api_key: Optional[str]):
    if API_KEY and api_key != API_KEY:
        raise HTTPException(status_code=401, detail="API key inválida")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_sync(req: ScrapeRequest):
    """Scrape síncrono — espera terminar. N8N precisa timeout alto (10+ min)."""
    _check_auth(req.api_key)

    queries = [f"{req.nicho} {c}" for c in req.cidades]
    logger.info(f"Iniciando scrape: {queries} (max={req.max_per_city}/cidade)")

    result = await scrape_multi(queries, req.max_per_city)
    companies = result["companies"]

    logger.info(
        f"Scrape concluído: {len(companies)} empresas | "
        f"bloqueado: {result['blocked']} | razão: {result['reason']}"
    )
    return ScrapeResponse(
        total=len(companies),
        companies=companies,
        blocked=result["blocked"],
        reason=result["reason"],
    )


# ─── Async mode (para N8N evitar timeout) ──────────────────────────────────
JOBS: dict[str, dict] = {}


async def _run_job(job_id: str, req: ScrapeRequest):
    JOBS[job_id]["status"] = "running"
    try:
        queries = [f"{req.nicho} {c}" for c in req.cidades]
        result = await scrape_multi(queries, req.max_per_city)
        JOBS[job_id]["status"] = "done"
        JOBS[job_id]["result"] = {
            "total": len(result["companies"]),
            "companies": result["companies"],
            "blocked": result["blocked"],
            "reason": result["reason"],
        }
    except Exception as e:
        logger.exception(f"Job {job_id} falhou")
        JOBS[job_id]["status"] = "error"
        JOBS[job_id]["error"] = str(e)


@app.post("/scrape/async")
async def scrape_async(req: ScrapeRequest, background_tasks: BackgroundTasks):
    _check_auth(req.api_key)
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {"status": "queued"}
    background_tasks.add_task(_run_job, job_id, req)
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    return {"job_id": job_id, **job}
