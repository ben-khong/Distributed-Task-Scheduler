/* This file runs when the Postgres database starts and it sets up the schema. 
The scheduler writes rows into this table, and the coordinator reads
from it to find tasks that need executing. The timestamp columns let you derive
the status of any task without storing an explicit status field. */

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    command TEXT NOT NULL,
    scheduled_at TIMESTAMP NOT NULL,
    picked_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    failed_at TIMESTAMP
);

CREATE INDEX idx_tasks_scheduled_at ON tasks (scheduled_at);