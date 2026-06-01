"""
Reaper — recovers jobs stuck in_progress due to worker crashes.

Runs every 60s. Any job that has been in_progress for longer than
STALE_THRESHOLD_SECONDS (default 5 min) is reset to pending and requeued.
This handles the case where a worker dies mid-job without updating the DB.
"""
import os
import time
import logging
import psycopg2
import psycopg2.extras
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [REAPER] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/healthex")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STALE_THRESHOLD_SECONDS = int(os.getenv("STALE_THRESHOLD_SECONDS", "300"))  # 5 min
REAPER_INTERVAL_SECONDS = int(os.getenv("REAPER_INTERVAL_SECONDS", "60"))
QUEUE_KEY = "queue:refresh_jobs"


def get_conn():
    return psycopg2.connect(DB_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def reap_stale_jobs(conn, r: redis.Redis):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE refresh_jobs SET
                status        = 'pending',
                claimed_by    = NULL,
                claimed_at    = NULL,
                next_retry_at = NOW()
            WHERE
                status     = 'in_progress'
                AND claimed_at < NOW() - (%s * INTERVAL '1 second')
            RETURNING id, priority, EXTRACT(EPOCH FROM scheduled_at) AS scheduled_ts
        """, (STALE_THRESHOLD_SECONDS,))

        reclaimed = cur.fetchall()
        conn.commit()

        for job in reclaimed:
            score = time.time()  # requeue as normal priority, due now
            r.zadd(QUEUE_KEY, {str(job["id"]): score})
            log.info("Reclaimed stale job %s, requeued.", job["id"])

        if reclaimed:
            log.info("Reaped %d stale job(s).", len(reclaimed))


def run():
    log.info("Reaper starting. Stale threshold: %ds, interval: %ds.",
             STALE_THRESHOLD_SECONDS, REAPER_INTERVAL_SECONDS)
    conn = get_conn()
    r = redis.from_url(REDIS_URL)
    try:
        while True:
            try:
                reap_stale_jobs(conn, r)
            except Exception as e:
                log.error("Reaper error: %s", e)
                time.sleep(1)
                conn = get_conn()
            time.sleep(REAPER_INTERVAL_SECONDS)
    finally:
        conn.close()


if __name__ == "__main__":
    run()
