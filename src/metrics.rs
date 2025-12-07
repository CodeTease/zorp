use std::sync::atomic::{AtomicUsize, Ordering};

pub static JOBS_QUEUED: AtomicUsize = AtomicUsize::new(0);
pub static JOBS_RUNNING: AtomicUsize = AtomicUsize::new(0);
pub static JOBS_COMPLETED: AtomicUsize = AtomicUsize::new(0);
pub static JOBS_FAILED: AtomicUsize = AtomicUsize::new(0);

pub fn inc_queued() { JOBS_QUEUED.fetch_add(1, Ordering::Relaxed); }
pub fn dec_queued() { JOBS_QUEUED.fetch_sub(1, Ordering::Relaxed); }

pub fn inc_running() { JOBS_RUNNING.fetch_add(1, Ordering::Relaxed); }
pub fn dec_running() { JOBS_RUNNING.fetch_sub(1, Ordering::Relaxed); }

pub fn inc_completed() { JOBS_COMPLETED.fetch_add(1, Ordering::Relaxed); }
pub fn inc_failed() { JOBS_FAILED.fetch_add(1, Ordering::Relaxed); }

pub fn get_metrics() -> String {
    let queued = JOBS_QUEUED.load(Ordering::Relaxed);
    let running = JOBS_RUNNING.load(Ordering::Relaxed);
    let completed = JOBS_COMPLETED.load(Ordering::Relaxed);
    let failed = JOBS_FAILED.load(Ordering::Relaxed);

    format!(
        "# HELP zorp_jobs_queued Current number of jobs in queue\n\
         # TYPE zorp_jobs_queued gauge\n\
         zorp_jobs_queued {}\n\
         # HELP zorp_jobs_running Current number of running jobs\n\
         # TYPE zorp_jobs_running gauge\n\
         zorp_jobs_running {}\n\
         # HELP zorp_jobs_completed_total Total number of successfully completed jobs\n\
         # TYPE zorp_jobs_completed_total counter\n\
         zorp_jobs_completed_total {}\n\
         # HELP zorp_jobs_failed_total Total number of failed jobs\n\
         # TYPE zorp_jobs_failed_total counter\n\
         zorp_jobs_failed_total {}\n",
        queued, running, completed, failed
    )
}
