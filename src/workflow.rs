use serde::{Deserialize, Serialize};
use crate::models::JobRequest;
use std::collections::HashMap;

#[cfg(test)]
#[path = "./workflow_tests.rs"]
mod tests;

#[derive(Debug, Deserialize, Serialize)]
pub struct Workflow {
    pub name: Option<String>,
    pub jobs: HashMap<String, WorkflowJob>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WorkflowJob {
    pub image: String,
    pub commands: Vec<String>,
    #[serde(default)]
    pub needs: Vec<String>,
    pub strategy: Option<Strategy>,
    #[serde(default)]
    pub services: Vec<crate::models::ServiceRequest>,
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Strategy {
    pub matrix: HashMap<String, Vec<String>>,
}

/// Represents the flattened graph of the workflow
#[derive(Debug)]
pub struct WorkflowGraph {
    /// All jobs involved in the workflow. Key is a temporary internal ID (e.g. job name + suffix).
    pub jobs: HashMap<String, JobRequest>,
    /// Dependency Map: Parent Job Key -> List of Child Job Keys
    pub dependencies: HashMap<String, Vec<String>>,
}

pub fn parse_workflow(yaml_content: &str) -> Result<WorkflowGraph, String> {
    let workflow: Workflow = serde_yaml::from_str(yaml_content)
        .map_err(|e| format!("Failed to parse YAML: {}", e))?;

    let mut job_map: HashMap<String, JobRequest> = HashMap::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    // 1. Expand Matrix and create JobRequests
    // To handle dependencies correctly with matrix expansion, we need to know the specific names of the expanded jobs.
    // If Job B needs Job A, and Job A has matrix (A1, A2), then Job B needs BOTH A1 and A2?
    // OR if Job B also has matrix (B1, B2), does B1 need A1?
    // Standard GitHub Actions behavior:
    // - If A is matrix, B (single) needs A (all variants).
    // - If A and B are matrix matching? Complexity high.
    // Let's assume for now: "needs: A" means "wait for all variants of A".

    // We first generate all job instances and give them unique keys.
    // Key format: "jobname" or "jobname-matrixSuffix"
    
    // We need a map from "Original Job Name" to "List of Expanded Job Keys"
    let mut expanded_keys_map: HashMap<String, Vec<String>> = HashMap::new();

    for (job_name, job_def) in &workflow.jobs {
        let expanded_jobs = expand_matrix(job_name, job_def);
        let mut keys = Vec::new();
        for (suffix, req) in expanded_jobs {
            let key = if suffix.is_empty() { job_name.clone() } else { format!("{}-{}", job_name, suffix) };
            job_map.insert(key.clone(), req);
            keys.push(key);
        }
        expanded_keys_map.insert(job_name.clone(), keys);
    }

    // 2. Resolve Dependencies
    for (job_name, job_def) in &workflow.jobs {
        let child_keys = expanded_keys_map.get(job_name).ok_or("Internal error")?;

        for needed_job_name in &job_def.needs {
            let parent_keys = expanded_keys_map.get(needed_job_name)
                .ok_or_else(|| format!("Job '{}' needs '{}', but it is not defined.", job_name, needed_job_name))?;

            // Fan-in: All children depend on all parents (by default)
            for parent_key in parent_keys {
                for child_key in child_keys {
                     dependencies.entry(parent_key.clone()).or_default().push(child_key.clone());
                }
            }
        }
    }

    Ok(WorkflowGraph {
        jobs: job_map,
        dependencies,
    })
}

fn expand_matrix(_name: &str, job_def: &WorkflowJob) -> Vec<(String, JobRequest)> {
    if let Some(strategy) = &job_def.strategy {
        if !strategy.matrix.is_empty() {
             // Cartesian product of matrix values
             let mut combinations: Vec<HashMap<String, String>> = vec![HashMap::new()];
             
             for (key, values) in &strategy.matrix {
                 let mut new_combinations = Vec::new();
                 for combo in combinations {
                     for val in values {
                         let mut new_combo = combo.clone();
                         new_combo.insert(key.clone(), val.clone());
                         new_combinations.push(new_combo);
                     }
                 }
                 combinations = new_combinations;
             }

             let mut requests = Vec::new();
             for combo in combinations {
                 let mut new_image = job_def.image.clone();
                 // Simple variable substitution
                 for (k, v) in &combo {
                     new_image = new_image.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                 }
                 
                 let mut new_commands = job_def.commands.clone();
                 for cmd in &mut new_commands {
                      for (k, v) in &combo {
                          *cmd = cmd.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                      }
                 }

                 let mut req = create_base_job_request(job_def);
                 req.image = new_image;
                 req.commands = new_commands;
                 
                 let mut env = req.env.clone().unwrap_or_default();
                 let mut suffix_parts = Vec::new();

                 // Deterministic sort for suffix generation
                 let mut sorted_keys: Vec<_> = combo.keys().collect();
                 sorted_keys.sort();

                 for k in sorted_keys {
                     let v = &combo[k];
                     env.insert(format!("MATRIX_{}", k.to_uppercase()), v.clone());
                     for (_, env_val) in env.iter_mut() {
                         *env_val = env_val.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                     }
                     suffix_parts.push(v.to_string()); // e.g. "14", "ubuntu"
                 }
                 req.env = Some(env);
                 
                 let suffix = suffix_parts.join("-");
                 requests.push((suffix, req));
             }
             return requests;
        }
    }

    vec![("".to_string(), create_base_job_request(job_def))]
}

fn create_base_job_request(job_def: &WorkflowJob) -> JobRequest {
    JobRequest {
        image: job_def.image.clone(),
        commands: job_def.commands.clone(),
        env: job_def.env.clone(),
        limits: None,
        callback_url: None,
        timeout_seconds: None,
        artifacts_path: None,
        user: None,
        cache_key: None,
        cache_paths: None,
        services: job_def.services.clone(),
        on_success: vec![], // No longer used for DAG
        debug: false,
        priority: None,
        enable_network: false,
        run_at: None,
    }
}
