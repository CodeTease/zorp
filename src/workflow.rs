use serde::{Deserialize, Serialize};
use crate::models::JobRequest;
use std::collections::{HashMap, HashSet};
use itertools::Itertools;
use regex::Regex;
use thiserror::Error;
use sha2::{Sha256, Digest};

#[cfg(test)]
#[path = "./workflow_tests.rs"]
mod tests;

#[derive(Error, Debug)]
pub enum WorkflowError {
    #[error("YAML syntax error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("Missing dependency: Job '{0}' needs '{1}', but it is not defined.")]
    MissingDependency(String, String),
    #[error("Cycle detected in workflow dependencies involving job: {0}")]
    CycleDetected(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

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

pub fn parse_workflow(yaml_content: &str) -> Result<WorkflowGraph, WorkflowError> {
    let workflow: Workflow = serde_yaml::from_str(yaml_content)?;

    let mut job_map: HashMap<String, JobRequest> = HashMap::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    // 1. Expand Matrix and create JobRequests
    let mut expanded_keys_map: HashMap<String, Vec<String>> = HashMap::new();

    for (job_name, job_def) in &workflow.jobs {
        let expanded_jobs = expand_matrix(job_name, job_def);
        let mut keys = Vec::new();
        for (suffix, req) in expanded_jobs {
            // Use original name if no matrix, otherwise append suffix (which is now a hash or structured)
            // But for readability in logs, we might want "job-ubuntu-14" style if possible.
            // The expand_matrix function returns a readable suffix.
            let key = if suffix.is_empty() { job_name.clone() } else { format!("{}-{}", job_name, suffix) };
            job_map.insert(key.clone(), req);
            keys.push(key);
        }
        expanded_keys_map.insert(job_name.clone(), keys);
    }

    // 2. Resolve Dependencies
    for (job_name, job_def) in &workflow.jobs {
        let child_keys = expanded_keys_map.get(job_name)
            .ok_or_else(|| WorkflowError::InternalError("Expanded keys not found".to_string()))?;

        for needed_job_name in &job_def.needs {
            let parent_keys = expanded_keys_map.get(needed_job_name)
                .ok_or_else(|| WorkflowError::MissingDependency(job_name.clone(), needed_job_name.clone()))?;

            // Fan-in: All children depend on all parents (by default)
            for parent_key in parent_keys {
                for child_key in child_keys {
                     dependencies.entry(parent_key.clone()).or_default().push(child_key.clone());
                }
            }
        }
    }

    // 3. Cycle Detection
    detect_cycles(&job_map, &dependencies)?;

    Ok(WorkflowGraph {
        jobs: job_map,
        dependencies,
    })
}

fn detect_cycles(
    jobs: &HashMap<String, JobRequest>, 
    dependencies: &HashMap<String, Vec<String>>
) -> Result<(), WorkflowError> {
    // Build adjacency list (Parent -> Children is what we have).
    // A cycle exists if we encounter a node currently in the recursion stack.
    let mut visited = HashSet::new();
    let mut recursion_stack = HashSet::new();

    // dependencies map is: Parent -> Children
    // We need to iterate all nodes. jobs keys are all nodes.
    for node in jobs.keys() {
        if detect_cycle_dfs(node, dependencies, &mut visited, &mut recursion_stack) {
            return Err(WorkflowError::CycleDetected(node.clone()));
        }
    }
    Ok(())
}

fn detect_cycle_dfs(
    node: &str,
    dependencies: &HashMap<String, Vec<String>>,
    visited: &mut HashSet<String>,
    recursion_stack: &mut HashSet<String>
) -> bool {
    if recursion_stack.contains(node) {
        return true;
    }
    if visited.contains(node) {
        return false;
    }

    visited.insert(node.to_string());
    recursion_stack.insert(node.to_string());

    if let Some(children) = dependencies.get(node) {
        for child in children {
            if detect_cycle_dfs(child, dependencies, visited, recursion_stack) {
                return true;
            }
        }
    }

    recursion_stack.remove(node);
    false
}

fn expand_matrix(_name: &str, job_def: &WorkflowJob) -> Vec<(String, JobRequest)> {
    if let Some(strategy) = &job_def.strategy {
        if !strategy.matrix.is_empty() {
             // Use itertools for Cartesian product
             // 1. Sort keys to ensure deterministic order
             let mut sorted_keys: Vec<_> = strategy.matrix.keys().collect();
             sorted_keys.sort();

             // 2. Prepare iterators
             let values_iter = sorted_keys.iter()
                 .map(|k| strategy.matrix.get(*k).unwrap().clone())
                 .multi_cartesian_product();

             let mut requests = Vec::new();

             for values in values_iter {
                 // values is Vec<String> corresponding to sorted_keys
                 let combo: HashMap<String, String> = sorted_keys.iter()
                     .zip(values.iter())
                     .map(|(k, v)| ((*k).clone(), v.clone()))
                     .collect();

                 let mut new_image = job_def.image.clone();
                 // Flexible variable substitution using Regex
                 for (k, v) in &combo {
                     let re = Regex::new(&format!(r"\$\{{\{{\s*matrix\.{}\s*\}}\}}", regex::escape(k))).unwrap();
                     new_image = re.replace_all(&new_image, v.as_str()).to_string();
                 }
                 
                 let mut new_commands = job_def.commands.clone();
                 for cmd in &mut new_commands {
                      for (k, v) in &combo {
                          let re = Regex::new(&format!(r"\$\{{\{{\s*matrix\.{}\s*\}}\}}", regex::escape(k))).unwrap();
                          *cmd = re.replace_all(cmd, v.as_str()).to_string();
                      }
                 }

                 let mut req = create_base_job_request(job_def);
                 req.image = new_image;
                 req.commands = new_commands;
                 
                 let mut env = req.env.clone().unwrap_or_default();
                 let mut suffix_parts = Vec::new();

                 for (k, v) in &combo {
                     env.insert(format!("MATRIX_{}", k.to_uppercase()), v.clone());
                     // Also substitute in ENV values if they exist
                     for (_, env_val) in env.iter_mut() {
                         let re = Regex::new(&format!(r"\$\{{\{{\s*matrix\.{}\s*\}}\}}", regex::escape(k))).unwrap();
                         *env_val = re.replace_all(env_val, v.as_str()).to_string();
                     }
                 }

                 // Generate readable suffix parts based on sorted keys
                 for k in &sorted_keys {
                     if let Some(v) = combo.get(*k) {
                         suffix_parts.push(v.clone());
                     }
                 }
                 
                 // Generate a Hash for robust uniqueness
                 let combo_str = suffix_parts.join("-");
                 let mut hasher = Sha256::new();
                 hasher.update(combo_str.as_bytes());
                 let result = hasher.finalize();
                 let hash_suffix = hex::encode(result);
                 // Shorten hash to 8 chars
                 let short_hash = &hash_suffix[..8];

                 // Readable suffix + Short Hash to avoid collision if values are similar but keys different?
                 // Actually standard GH actions just uses values. 
                 // But user suggested: "Use hash as suffix for internal ID, but keep display name readable".
                 // Here we return the suffix used for the KEY in the map.
                 // So let's use readable values + short hash for safety.
                 let suffix = format!("{}-{}", combo_str, short_hash);
                 
                 req.env = Some(env);
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
        matrix: None,
    }
}
