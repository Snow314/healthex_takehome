# Database Schema

```sql
CREATE TABLE patients (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    external_id VARCHAR(255) NOT NULL UNIQUE,  -- ID used at the EHR
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE studies (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name              VARCHAR(255) NOT NULL,
    refresh_frequency INTERVAL NOT NULL,  -- e.g. '1 day', '7 days'
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Which patients are enrolled in which studies
CREATE TABLE patient_study_enrollments (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id        UUID NOT NULL REFERENCES patients(id),
    study_id          UUID NOT NULL REFERENCES studies(id),
    enrolled_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consent_expires_at TIMESTAMPTZ,
    last_refreshed_at TIMESTAMPTZ,
    UNIQUE (patient_id, study_id)
);

-- EHR endpoints and their rate limits
CREATE TABLE ehr_endpoints (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                     VARCHAR(255) NOT NULL,  -- e.g. 'epic', 'cerner'
    base_url                 VARCHAR(500) NOT NULL,
    rate_limit_rpm           INT NOT NULL DEFAULT 30,
    avg_response_time_seconds FLOAT NOT NULL DEFAULT 5.0,
    response_sample_count    INT NOT NULL DEFAULT 0   -- used for rolling average
);

-- Which EHR endpoints hold data for a given patient
CREATE TABLE patient_ehr_endpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id      UUID NOT NULL REFERENCES patients(id),
    ehr_endpoint_id UUID NOT NULL REFERENCES ehr_endpoints(id),
    UNIQUE (patient_id, ehr_endpoint_id)
);

CREATE TYPE job_status AS ENUM ('pending', 'in_progress', 'completed', 'failed');
CREATE TYPE job_priority AS ENUM ('low', 'normal', 'high', 'urgent');
CREATE TYPE failure_type AS ENUM ('transient', 'permanent');

CREATE TABLE refresh_jobs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id        UUID NOT NULL REFERENCES patients(id),
    study_id          UUID NOT NULL REFERENCES studies(id),
    ehr_endpoint_id   UUID NOT NULL REFERENCES ehr_endpoints(id),

    status            job_status   NOT NULL DEFAULT 'pending',
    priority          job_priority NOT NULL DEFAULT 'normal',

    -- scheduling
    scheduled_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,

    -- retry / failure
    attempt_count     INT NOT NULL DEFAULT 0,
    max_attempts      INT NOT NULL DEFAULT 3,
    next_retry_at     TIMESTAMPTZ,
    failure_type      failure_type,
    failure_reason    TEXT,

    -- worker coordination (claimed_by is the worker ID)
    claimed_by        VARCHAR(255),
    claimed_at        TIMESTAMPTZ,

    -- upstream job ID returned by the EHR mock
    ehr_job_id        VARCHAR(255),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW())

CREATE UNIQUE INDEX idx_refresh_jobs_no_duplicates
    ON refresh_jobs (patient_id, study_id, ehr_endpoint_id)
    WHERE status IN ('pending', 'in_progress');

-- Speeds up the worker claim query (priority + scheduled_at ordering)
CREATE INDEX idx_refresh_jobs_claimable
    ON refresh_jobs (priority DESC, scheduled_at ASC)
    WHERE status = 'pending';

-- Speeds up the scheduler query finding patients due for refresh
CREATE INDEX idx_enrollments_last_refreshed
    ON patient_study_enrollments (last_refreshed_at ASC NULLS FIRST);
```
