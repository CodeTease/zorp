use lazy_static::lazy_static;
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, Encoder, IntCounter,
    IntCounterVec, IntGauge, TextEncoder,
};
use tracing::error;

lazy_static! {
    pub static ref JOBS_QUEUED: IntGauge = register_int_gauge!(
        "zorp_jobs_queued",
        "Current number of jobs in queue"
    ).expect("metric can be created");

    pub static ref JOBS_RUNNING: IntGauge = register_int_gauge!(
        "zorp_jobs_running",
        "Current number of running jobs"
    ).expect("metric can be created");

    pub static ref JOBS_COMPLETED: IntCounter = register_int_counter!(
        "zorp_jobs_completed_total",
        "Total number of successfully completed jobs"
    ).expect("metric can be created");

    pub static ref JOBS_FAILED: IntCounterVec = register_int_counter_vec!(
        "zorp_jobs_failed_total",
        "Total number of failed jobs",
        &["reason"]
    ).expect("metric can be created");
}

pub fn inc_queued() {
    JOBS_QUEUED.inc();
}

pub fn dec_queued() {
    JOBS_QUEUED.dec();
}

pub fn inc_running() {
    JOBS_RUNNING.inc();
}

pub fn dec_running() {
    JOBS_RUNNING.dec();
}

pub fn get_running() -> usize {
    JOBS_RUNNING.get() as usize
}

pub fn inc_completed() {
    JOBS_COMPLETED.inc();
}

pub fn inc_failed(reason: &str) {
    JOBS_FAILED.with_label_values(&[reason]).inc();
}

pub fn get_metrics() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => match String::from_utf8(buffer) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to convert metrics to UTF8: {}", e);
                String::new()
            }
        },
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_format() {
        // Increment metrics
        inc_queued();
        inc_running();
        inc_completed();
        inc_failed("timeout");

        let output = get_metrics();
        
        // Basic assertions to ensure Prometheus format
        assert!(output.contains("# HELP zorp_jobs_queued"));
        assert!(output.contains("# TYPE zorp_jobs_queued gauge"));
        
        assert!(output.contains("# HELP zorp_jobs_running"));
        assert!(output.contains("# TYPE zorp_jobs_running gauge"));

        assert!(output.contains("# HELP zorp_jobs_completed_total"));
        assert!(output.contains("# TYPE zorp_jobs_completed_total counter"));

        assert!(output.contains("# HELP zorp_jobs_failed_total"));
        assert!(output.contains("# TYPE zorp_jobs_failed_total counter"));
        // Check for label
        assert!(output.contains("zorp_jobs_failed_total{reason=\"timeout\"}"));
    }
}
