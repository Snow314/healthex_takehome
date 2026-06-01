#!/bin/bash
# Local dev: Python services run directly (no rebuild on changes).
# Starts postgres + redis via Docker if not already running.

set -e

export DATABASE_URL="postgresql://healthex:healthex@localhost:5433/healthex"
export REDIS_URL="redis://localhost:6379/0"
export EHR_API_URL="http://localhost:8000"
export SCHEDULER_INTERVAL_SECONDS="5"
export LOOP_INTERVAL_SECONDS="10"

echo "Stopping any previous services..."
pkill -f "src.scheduler" 2>/dev/null || true
pkill -f "src.worker"    2>/dev/null || true
pkill -f "src.reaper"    2>/dev/null || true
pkill -f "mock_ehr_api"  2>/dev/null || true
sleep 0.5

echo "Starting infrastructure..."
docker-compose -f docker-compose.dev.yml up -d

echo "Waiting for postgres..."
until docker exec takehome-postgres-1 pg_isready -U healthex -q; do sleep 1; done

source venv/bin/activate

echo "Seeding database..."
python -m src.seed

echo "Resetting job state..."
python -c "
import psycopg2, redis, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute('TRUNCATE refresh_jobs')
cur.execute('UPDATE patient_study_enrollments SET last_refreshed_at = NULL')
cur.execute('UPDATE ehr_endpoints SET avg_response_time_seconds = 2.0, response_sample_count = 0')
conn.commit()
conn.close()
redis.from_url(os.environ['REDIS_URL']).flushdb()
"

echo "Starting services..."
uvicorn src.mock_ehr_api:app --port 8000 --log-level warning &

echo "Waiting for mock EHR..."
until curl -sf http://localhost:8000/health > /dev/null 2>&1 || curl -sf -X POST http://localhost:8000/patients/ping/\$updateData > /dev/null 2>&1; do sleep 0.5; done
echo "Mock EHR ready."

python -m src.scheduler &
python -m src.worker &
python -m src.worker &
python -m src.reaper &

echo ""
echo "All services running."
echo "  Mock EHR: http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop all."

trap "kill 0" SIGINT
wait
