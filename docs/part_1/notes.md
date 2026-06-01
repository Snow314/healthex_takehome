# Notes & Design Decisions

## Core Challenges

### 1. Scheduling
- Scheduler polls DB for patients where `last_refreshed_at < NOW() - refresh_frequency`
- Creates refresh jobs, skips duplicates via ON CONFLICT DO NOTHING

### 2. Prioritization
- Jobs have priority enum: urgent, high, normal, low
- Redis sorted set score = priority offset + scheduled_at timestamp
- Urgent cases (consent expiring, user-triggered) get lower score → claimed first

### 3. Rate Limiting
- Redis token bucket per EHR endpoint, refills every 60s to `rate_limit_rpm`
- Lua script ensures atomic decrement across workers
- Rate limited jobs requeued with 60s delay
- Refill formula: `tokens = min(rpm, tokens + rpm × (now − last_refill) / 60)`

### 4. Failure Handling
- Transient (429, 503): retry with exponential backoff (30s → 120s → 300s), max 3 attempts
- Permanent (422): marked failed immediately, no retry
- Status poll timeout treated as transient

### 5. Distribution
- Thundering Heard avoided automatically
    - Redis queue naturally smooths bursts — workers consume at their own pace

### 6. Horizontal Scaling
- BZPOPMIN atomically removes job from queue — only one worker gets it
- Postgres mark_in_progress rowcount check as safety net
- Stateless workers, auto scale by adding more processes based on queue length

### 7. Cost
- Duplicate jobs blocked at DB level (partial unique index on pending/in_progress)
- Average response time tracked per endpoint — delays first status poll to reduce GET requests
- Scheduler skips patients already having a pending/in_progress job


## Other Tradeoffs/ Discussions

## Scheduler: Polling vs SQS
- Using DB polling for simplicity, works fine at low scale
- SQS delayed visibility eliminates the scheduler process entirely at scale
- EventBridge for delays > 15min SQS max

## Job Queue: Redis vs SQS
- Redis sorted set, workers block on BZPOPMIN — no polling
- Score = priority + scheduled_at, lowest score claimed first
- SQS better durability but adds AWS dependency and loses co-located rate limiter

## Refresh Frequency
- INTERVAL sufficient for "every N days" — simple SQL eligibility check
- Cron expression if studies need time-of-day pinning (adds croniter dep)

## Fault Tolerance
- Backup Redis Replica only used if the main one goes down and needs to be elected
- Stale in-progress jobs need a timeout reaper (worker crash recovery)
- Dead letter queue for permanently failed jobs at scale
