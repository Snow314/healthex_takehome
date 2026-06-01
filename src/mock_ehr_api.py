"""
Mock EHR API — simulates Epic/Cerner refresh endpoints for local development.

POST /patients/{id}/$updateData   — triggers a refresh, returns a job_id
GET  /patients/{id}/data-retrieval/status?job_id=  — polls job completion

Outcome distribution on trigger:
  75% success (job accepted, poll to completion)
  12% transient failure (503 — retry with backoff)
   8% rate limited (429 — retry after 60s)
   5% permanent failure (422 — do not retry)

Status polling resolves in_progress → completed 80% of the time per poll.
"""
import random
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Mock EHR API")

# In-memory store of retrieval jobs: job_id -> status
_jobs: dict[str, str] = {}

# Outcome weights: (outcome, weight)
# mostly success, occasional transient/rate-limit, rare permanent failure
_OUTCOMES = [
    ("success", 75),
    ("transient_failure", 12),
    ("rate_limited", 8),
    ("permanent_failure", 5),
]
_OUTCOME_CHOICES = [o for o, w in _OUTCOMES for _ in range(w)]


def _roll_outcome() -> str:
    return random.choice(_OUTCOME_CHOICES)


class UpdateDataResponse(BaseModel):
    job_id: str
    status: str
    message: str


class StatusResponse(BaseModel):
    job_id: str
    status: str
    message: str


@app.post("/patients/{patient_id}/$updateData", response_model=UpdateDataResponse)
def trigger_update(patient_id: str):
    outcome = _roll_outcome()

    if outcome == "rate_limited":
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Retry after 60 seconds.")

    if outcome == "permanent_failure":
        raise HTTPException(status_code=422, detail="Unprocessable patient record. Refresh not supported for this patient.")

    if outcome == "transient_failure":
        raise HTTPException(status_code=503, detail="EHR service temporarily unavailable.")

    # success — create a job
    job_id = str(uuid.uuid4())
    _jobs[job_id] = "in_progress"
    return UpdateDataResponse(job_id=job_id, status="accepted", message="Refresh job started.")


@app.get("/patients/{patient_id}/data-retrieval/status", response_model=StatusResponse)
def get_status(patient_id: str, job_id: str):
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail="Job not found.")

    current = _jobs[job_id]

    # Simulate progress: in_progress jobs complete ~80% of the time when polled
    if current == "in_progress":
        if random.random() < 0.8:
            _jobs[job_id] = "completed"
            current = "completed"

    return StatusResponse(job_id=job_id, status=current, message=f"Job is {current}.")
