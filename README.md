# Patient Data Refresh Orchestrator

Schedules, executes, and tracks EHR data refresh jobs across multiple patients, studies, and endpoints.

## Directory Structure

```
src/
  mock_ehr_api.py   # Simulated EHR API (success, transient, rate-limit, permanent failures)
  scheduler.py      # Polls for due patients, creates jobs, pushes to Redis queue
  worker.py         # Claims jobs, calls EHR API, handles retries and rate limiting
  reaper.py         # Recovers stale in-progress jobs from crashed workers
  seed.py           # Populates DB with patients, studies, enrollments, endpoints

db/
  init.sql          # Schema — runs automatically on first Postgres start

docs/
  part_1/
    notes.md        # Design decisions and tradeoffs
    db_schema.md    # Schema documentation
  part_2/
    part_2.md       # Scaling design (5K → 10M patients)
    part_2_print.html  # Printable one-pager (open in browser, Cmd+P → Save as PDF)
```

## Architecture

- **Scheduler** — polls for due patients and enqueues refresh jobs
- **Workers** (x2) — claim jobs from Redis queue, call EHR API, handle retries
- **Reaper** — recovers stale in-progress jobs from crashed workers
- **Mock EHR API** — simulates Epic/Cerner with realistic failure rates (75% success, 12% transient, 8% rate-limited, 5% permanent)

## Running

### Option 1 — Docker (everything from scratch)

```bash
docker-compose up --build
```

To wipe state and restart clean:

```bash
docker-compose down -v && docker-compose up --build
```

### Option 2 — Local (no rebuild on code changes)

Starts Postgres (port 5433) and Redis (port 6379) via Docker, then runs all Python services directly:

```bash
./start.sh
```

To run services individually in separate terminals:

```bash
source venv/bin/activate
export DATABASE_URL="postgresql://healthex:healthex@localhost:5433/healthex"
export REDIS_URL="redis://localhost:6379/0"
export EHR_API_URL="http://localhost:8000"

python -m src.seed
uvicorn src.mock_ehr_api:app --port 8000 --reload
python -m src.scheduler
python -m src.worker
python -m src.reaper
```

### Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## What I'd Do With More Time

- **Priority assignment** — currently all jobs default to `normal`. Should compute priority from `consent_expires_at` proximity and flag user-triggered refreshes as `urgent`.
- **Observability** — structured logging with job IDs, emit metrics (queue depth, worker throughput, error rate by endpoint) to Datadog or CloudWatch.
- **Graceful shutdown** — workers should finish the current job before exiting on SIGTERM rather than being killed mid-flight.
- **Tests** — unit tests for scheduling logic and the rate limiter; integration test that spins up the stack and verifies jobs move through all states end to end.
