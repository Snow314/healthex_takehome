"""
Scheduler — periodically finds patients due for a data refresh and enqueues jobs.

Eligibility: enrollment where last_refreshed_at IS NULL or older than refresh_frequency.
Deduplication: INSERT ON CONFLICT DO NOTHING — skips patients with an existing
               pending or in_progress job (enforced by partial unique index).
Queue: jobs are pushed to a Redis sorted set scored by priority + scheduled_at
       so urgent jobs always surface first.
"""
import os
import time
import logging
import psycopg2
import psycopg2.extras
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SCHEDULER] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/healthex")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POLL_INTERVAL_SECONDS = int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "5"))
QUEUE_KEY = "queue:refresh_jobs"

PRIORITY_SCORES = {"urgent": 0, "high": 1, "normal": 2, "low": 3}


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def make_score(priority: str, scheduled_at_ts: float) -> float:
    # 1e10 keeps priority bands well above any plausible unix timestamp (~1.7e9),
    # so urgent jobs always sort before normal ones regardless of scheduled time.
    return PRIORITY_SCORES.get(priority, 2) * 1e10 + scheduled_at_ts


def schedule_due_patients(conn, r: redis.Redis):
    # Finds every enrollment whose last_refreshed_at is NULL or older than the
    # study's refresh_frequency, then inserts a job for each one.
    # ON CONFLICT DO NOTHING relies on a partial unique index on
    # (patient_id, study_id) WHERE status IN ('pending', 'in_progress') —
    # completed/failed jobs don't block new ones for the same enrollment.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                pse.patient_id,
                pse.study_id,
                pee.ehr_endpoint_id
            FROM patient_study_enrollments pse
            JOIN patient_ehr_endpoints pee ON pee.patient_id = pse.patient_id
            JOIN studies s ON s.id = pse.study_id
            WHERE
                pse.last_refreshed_at IS NULL
                OR pse.last_refreshed_at < NOW() - s.refresh_frequency
        """)
        due = cur.fetchall()

        if not due:
            log.info("No patients due for refresh.")
            return

        new_jobs = []
        for row in due:
            cur.execute("""
                INSERT INTO refresh_jobs (patient_id, study_id, ehr_endpoint_id)
                VALUES (%(patient_id)s, %(study_id)s, %(ehr_endpoint_id)s)
                ON CONFLICT DO NOTHING
                RETURNING id, priority, EXTRACT(EPOCH FROM scheduled_at) AS scheduled_ts
            """, row)
            job = cur.fetchone()
            if job:
                new_jobs.append(job)

        conn.commit()  # commit before touching Redis — prevents phantom IDs if commit fails

        for job in new_jobs:
            score = make_score(job["priority"], float(job["scheduled_ts"]))
            r.zadd(QUEUE_KEY, {str(job["id"]): score})

        pushed = len(new_jobs)
        log.info("Pushed %d new job(s) to queue from %d due patient(s).", pushed, len(due))


def push_pending_retries(conn, r: redis.Redis):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, priority, EXTRACT(EPOCH FROM next_retry_at) AS scheduled_ts
            FROM refresh_jobs
            WHERE status = 'pending'
              AND next_retry_at IS NOT NULL
              AND next_retry_at <= NOW()
        """)
        due = cur.fetchall()
    for job in due:
        score = make_score(job["priority"], float(job["scheduled_ts"]))
        r.zadd(QUEUE_KEY, {str(job["id"]): score})
    if due:
        log.info("Pushed %d retry job(s) to queue.", len(due))


def run():
    log.info("Scheduler starting. Poll interval: %ds.", POLL_INTERVAL_SECONDS)
    conn = get_conn()
    r = redis.from_url(REDIS_URL)
    try:
        while True:
            try:
                schedule_due_patients(conn, r)
                push_pending_retries(conn, r)
            except Exception as e:
                log.error("Scheduler error: %s", e)
                time.sleep(1)
                conn = get_conn()
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
