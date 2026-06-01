"""
Worker — claims and processes refresh jobs from the Redis queue.

Flow per job:
  1. BZPOPMIN blocks until a job is available (no polling).
  2. mark_in_progress: UPDATE WHERE status='pending' — rowcount check prevents
     double-claim if two workers race on the same job.
  3. Rate limit check via Redis Lua token bucket (per endpoint, refills continuously).
  4. POST /patients/{id}/$updateData — trigger the EHR refresh.
  5. Poll /patients/{id}/data-retrieval/status until completed or timeout.
  6. On success: mark completed, update last_refreshed_at, update rolling avg response time.
  7. On transient failure (429/503/timeout): exponential backoff (30s→120s→300s), max 3 attempts.
  8. On permanent failure (422): mark failed, no retry.

Multiple workers run concurrently — Redis BZPOPMIN + Postgres rowcount check ensure
each job is processed exactly once.
"""
import os
import time
import uuid
import logging
import requests
import psycopg2
import psycopg2.extras
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WORKER] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/healthex")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
EHR_API_URL = os.getenv("EHR_API_URL", "http://localhost:8000")
WORKER_ID = str(uuid.uuid4())
QUEUE_KEY = "queue:refresh_jobs"
STATUS_POLL_RETRIES = 3
STATUS_POLL_DELAY = 1

PRIORITY_SCORES = {"urgent": 0, "high": 1, "normal": 2, "low": 3}


def make_score(priority: str, scheduled_ts: float) -> float:
    return PRIORITY_SCORES.get(priority, 2) * 1e10 + scheduled_ts

# Lua token bucket: drip tokens in proportion to elapsed time, decrement if available
RATE_LIMIT_SCRIPT = """
local key    = KEYS[1]
local rate   = tonumber(ARGV[1])
local now    = tonumber(ARGV[2])

local data       = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens     = tonumber(data[1]) or rate
local last_refill = tonumber(data[2]) or now

tokens = math.min(rate, tokens + rate * (now - last_refill) / 60)

if tokens >= 1 then
    redis.call('HMSET', key, 'tokens', tokens - 1, 'last_refill', now)
    redis.call('EXPIRE', key, 120)
    return 1
end

redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
return 0
"""


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def fetch_job(conn, job_id: str):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM refresh_jobs WHERE id = %s", (job_id,))
        return cur.fetchone()


def mark_in_progress(conn, job_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE refresh_jobs
            SET status = 'in_progress', claimed_by = %s, claimed_at = NOW(), started_at = NOW()
            WHERE id = %s AND status = 'pending'
        """, (WORKER_ID, job_id))
        conn.commit()
        return cur.rowcount == 1  # False if another worker already claimed it


def complete_job(conn, job_id: str, elapsed_seconds: float):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE refresh_jobs SET status = 'completed', completed_at = NOW() WHERE id = %s
        """, (job_id,))
        cur.execute("""
            UPDATE patient_study_enrollments SET last_refreshed_at = NOW()
            WHERE patient_id = (SELECT patient_id FROM refresh_jobs WHERE id = %s)
              AND study_id   = (SELECT study_id   FROM refresh_jobs WHERE id = %s)
        """, (job_id, job_id))
        cur.execute("""
            UPDATE ehr_endpoints
            SET
                response_sample_count     = response_sample_count + 1,
                avg_response_time_seconds = avg_response_time_seconds
                    + (%s - avg_response_time_seconds) / (response_sample_count + 1)
            WHERE id = (SELECT ehr_endpoint_id FROM refresh_jobs WHERE id = %s)
        """, (elapsed_seconds, job_id))
        conn.commit()


def fail_job(conn, r: redis.Redis, job_id: str, reason: str, failure_type: str):
    with conn.cursor() as cur:
        if failure_type == "transient":
            cur.execute("""
                UPDATE refresh_jobs SET
                    status         = CASE WHEN attempt_count + 1 >= max_attempts THEN 'failed'::job_status ELSE 'pending'::job_status END,
                    attempt_count  = attempt_count + 1,
                    failure_type   = %s::failure_type,
                    failure_reason = %s,
                    next_retry_at  = NOW() + (
                        CASE LEAST(attempt_count + 1, 3)
                            WHEN 1 THEN 30
                            WHEN 2 THEN 120
                            ELSE 300
                        END * INTERVAL '1 second'
                    ),
                    claimed_by     = NULL,
                    claimed_at     = NULL
                WHERE id = %s
                RETURNING status, priority, EXTRACT(EPOCH FROM next_retry_at) AS retry_ts
            """, (failure_type, reason, job_id))
            row = cur.fetchone()
            conn.commit()
            if row and row["status"] == "pending":
                score = make_score(row["priority"], float(row["retry_ts"]))
                r.zadd(QUEUE_KEY, {job_id: score})
        else:
            cur.execute("""
                UPDATE refresh_jobs SET
                    status = 'failed'::job_status, failure_type = %s::failure_type, failure_reason = %s, completed_at = NOW()
                WHERE id = %s
            """, (failure_type, reason, job_id))
            conn.commit()


def requeue_job(r: redis.Redis, job_id: str, delay_seconds: int = 60):
    """Push job back onto the queue with a future score so it's picked up after the delay."""
    score = time.time() + delay_seconds
    r.zadd(QUEUE_KEY, {job_id: score})


def check_rate_limit(r: redis.Redis, endpoint_id: str, rate_limit_rpm: int) -> bool:
    key = f"ratelimit:{endpoint_id}"
    allowed = r.eval(RATE_LIMIT_SCRIPT, 1, key, rate_limit_rpm, int(time.time()))
    return bool(allowed)


def get_endpoint(conn, job_id: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e.id, e.rate_limit_rpm, e.avg_response_time_seconds
            FROM ehr_endpoints e
            JOIN refresh_jobs j ON j.ehr_endpoint_id = e.id
            WHERE j.id = %s
        """, (job_id,))
        return cur.fetchone()


def poll_status(patient_id: str, ehr_job_id: str, initial_wait: float) -> str:
    time.sleep(min(initial_wait, 3.0))  # cap at 3s regardless of rolling avg
    for _ in range(STATUS_POLL_RETRIES):
        resp = requests.get(
            f"{EHR_API_URL}/patients/{patient_id}/data-retrieval/status",
            params={"job_id": ehr_job_id},
            timeout=10,
        )
        if resp.status_code == 200 and resp.json()["status"] == "completed":
            return "completed"
        time.sleep(STATUS_POLL_DELAY)
    return "timeout"


def process_job(conn, r: redis.Redis, job_id: str):
    job = fetch_job(conn, job_id)
    if not job:
        log.warning("Job %s not found in DB, skipping.", job_id)
        return

    if not mark_in_progress(conn, job_id):
        log.warning("Job %s already claimed, skipping.", job_id)
        return

    patient_id = str(job["patient_id"])
    log.info("Processing job %s for patient %s (attempt %d)", job_id, patient_id, job["attempt_count"] + 1)

    endpoint = get_endpoint(conn, job_id)
    if not check_rate_limit(r, str(endpoint["id"]), endpoint["rate_limit_rpm"]):
        log.warning("Job %s rate limited at endpoint level, requeueing.", job_id)
        with conn.cursor() as cur:
            cur.execute("UPDATE refresh_jobs SET status = 'pending', claimed_by = NULL, claimed_at = NULL WHERE id = %s", (job_id,))
            conn.commit()
        r.zadd(QUEUE_KEY, {job_id: make_score(job["priority"], time.time() + 60)})
        return

    try:
        resp = requests.post(f"{EHR_API_URL}/patients/{patient_id}/$updateData", timeout=10)
    except requests.RequestException as e:
        log.warning("Job %s connection error: %s", job_id, e)
        fail_job(conn, r, job_id, str(e), "transient")
        return

    if resp.status_code == 200:
        ehr_job_id = resp.json()["job_id"]
        start = time.time()
        status = poll_status(patient_id, ehr_job_id, endpoint["avg_response_time_seconds"])
        elapsed = time.time() - start
        if status == "completed":
            log.info("Job %s completed in %.1fs (patient %s).", job_id, elapsed, patient_id)
            complete_job(conn, job_id, elapsed)
        else:
            log.warning("Job %s poll timed out after %.1fs.", job_id, elapsed)
            fail_job(conn, r, job_id, "Status polling timed out", "transient")

    elif resp.status_code == 429:
        log.warning("Job %s rate limited by EHR endpoint, requeueing with 60s delay.", job_id)
        fail_job(conn, r, job_id, "Rate limited", "transient")

    elif resp.status_code == 503:
        log.warning("Job %s transient EHR failure (503), will retry.", job_id)
        fail_job(conn, r, job_id, "EHR temporarily unavailable", "transient")

    elif resp.status_code == 422:
        log.error("Job %s permanent failure (422): %s", job_id, resp.json().get("detail", ""))
        fail_job(conn, r, job_id, resp.json().get("detail", "Permanent failure"), "permanent")

    else:
        log.error("Job %s unexpected HTTP %d.", job_id, resp.status_code)
        fail_job(conn, r, job_id, f"Unexpected HTTP {resp.status_code}", "transient")


def run():
    log.info("Worker %s starting.", WORKER_ID)
    conn = get_conn()
    r = redis.from_url(REDIS_URL)
    try:
        while True:
            try:
                result = r.bzpopmin(QUEUE_KEY, timeout=5)
                if result:
                    _, job_id_bytes, score = result
                    # Scores encode priority*1e10 + unix_ts. Extract the timestamp
                    # to check whether this job's scheduled time has arrived yet.
                    scheduled_ts = score % 1e10 if score >= 1e10 else score
                    now = time.time()
                    if scheduled_ts > now + 0.5:
                        r.zadd(QUEUE_KEY, {job_id_bytes.decode(): score})
                        time.sleep(min(scheduled_ts - now, 5))
                        continue
                    process_job(conn, r, job_id_bytes.decode())
                else:
                    log.debug("Queue empty, waiting...")
            except Exception as e:
                log.error("Worker error: %s", e)
                time.sleep(1)
                conn = get_conn()
    finally:
        conn.close()


if __name__ == "__main__":
    run()
