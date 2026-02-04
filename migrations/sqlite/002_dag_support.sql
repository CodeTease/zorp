CREATE TABLE IF NOT EXISTS job_dependencies (
    parent_job_id TEXT NOT NULL,
    child_job_id TEXT NOT NULL,
    PRIMARY KEY (parent_job_id, child_job_id),
    FOREIGN KEY (parent_job_id) REFERENCES jobs(id),
    FOREIGN KEY (child_job_id) REFERENCES jobs(id)
);

ALTER TABLE jobs ADD COLUMN dependencies_met INTEGER DEFAULT 0;
ALTER TABLE jobs ADD COLUMN workflow_id TEXT;
