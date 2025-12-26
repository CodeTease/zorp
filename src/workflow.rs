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

pub fn parse_workflow(yaml_content: &str) -> Result<Vec<JobRequest>, String> {
    let workflow: Workflow = serde_yaml::from_str(yaml_content)
        .map_err(|e| format!("Failed to parse YAML: {}", e))?;

    let mut job_map: HashMap<String, Vec<JobRequest>> = HashMap::new();
    let mut dependencies: HashMap<String, Vec<String>> = HashMap::new();

    // 1. First pass: Expand Matrix and create JobRequests
    for (job_name, job_def) in &workflow.jobs {
        let expanded_jobs = expand_matrix(job_name, job_def);
        job_map.insert(job_name.clone(), expanded_jobs);
        dependencies.insert(job_name.clone(), job_def.needs.clone());
    }

    // 2. Second pass: Resolve Dependencies (build the tree)
    // We need to identify root jobs (those with no needs or empty needs).
    // And for each job, we need to append it to the on_success of its 'needs'.

    // This is tricky because `on_success` is owned. We need to construct the graph.
    // However, the current model is: Job A -> on_success: [Job B]
    // If Job C needs Job B, then Job B -> on_success: [Job C].
    // If Job C needs Job A, then Job A -> on_success: [Job B, Job C].
    
    // The user example: Build -> Test (needs Build).
    // "Thấy test cần build -> gán 2 job test vào on_success của build."

    // Let's invert the dependency graph to find "dependents".
    // dependents[X] = [Y, Z] means Y and Z depend on X.
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for (job_name, needs) in &dependencies {
        if needs.is_empty() {
             // Root job
        } else {
            for needed_job in needs {
                dependents.entry(needed_job.clone()).or_default().push(job_name.clone());
            }
        }
    }

    // We also need to check for cycles, but let's assume valid DAG for now.
    // We need to build the JobRequests from the bottom up (or recursively).
    // Actually, since `JobRequest` owns `on_success`, we can't easily link them if we build top-down without RefCell or similar.
    // But since it's a tree/forest of JobRequests, we can build it.
    
    // Wait, if C depends on A and B, we can't represent that with simple `on_success` chaining 
    // UNLESS we duplicate C (run C after A, and run C after B).
    // The user acknowledges this limitation ("Thách thức: Cái khó là Fan-in").
    // The user says: "Thấy test cần build -> gán 2 job test vào on_success của build."
    // This implies a tree structure. 
    // If we have diamond dependencies (A -> B, A -> C, B -> D, C -> D), D would run twice?
    // For now, let's assume a tree structure or just duplicate jobs (fan-out only).

    // Algorithm:
    // 1. Identify all jobs.
    // 2. Sort them topologically? Or just build recursively?
    // Since we need to embed children into parents, we should build children first?
    // No, we can't build children first because they might depend on multiple parents (which we said we might duplicate).
    
    // Let's try a recursive approach.
    // build_job_tree(job_name): returns Vec<JobRequest> (the expanded jobs for this name, with their children attached)

    // To avoid infinite recursion on cycles, we should track visited path? (assuming DAG).
    // But since we are constructing a tree, if there is a diamond, we will visit D twice.
    
    // We need to know who are the "roots" of the workflow to return them.
    // Roots are jobs that have no dependencies (empty `needs`).

    let mut root_jobs: Vec<JobRequest> = Vec::new();
    let mut visited_recursion = std::collections::HashSet::new();
    let mut visited_global = std::collections::HashSet::new();

    for (name, deps) in &dependencies {
        if deps.is_empty() {
             visited_recursion.clear();
             let mut roots = build_job_hierarchy(name, &job_map, &dependents, &mut visited_recursion, &mut visited_global)?;
             root_jobs.append(&mut roots);
        }
    }

    // If we have jobs defined but found no roots, or didn't visit all jobs, we might have a cycle (e.g. A <-> B).
    // In a valid DAG, every node is reachable from some root (assuming connected, or multiple roots).
    // If there is a cycle like A->B->A, neither A nor B is a root (needs is not empty).
    if root_jobs.is_empty() && !workflow.jobs.is_empty() {
        return Err("Circular dependency detected: No root jobs found (all jobs have dependencies).".to_string());
    }

    if visited_global.len() != workflow.jobs.len() {
        // Find which jobs were not visited
        let all_jobs: std::collections::HashSet<String> = workflow.jobs.keys().cloned().collect();
        let unvisited: Vec<_> = all_jobs.difference(&visited_global).collect();
        return Err(format!("Circular dependency detected or unreachable jobs: {:?}", unvisited));
    }

    Ok(root_jobs)
}

fn expand_matrix(_name: &str, job_def: &WorkflowJob) -> Vec<JobRequest> {
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
                 // Simple variable substitution for ${{ matrix.key }}
                 for (k, v) in &combo {
                     new_image = new_image.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                 }
                 
                 // Also substitute in env vars? For now just image as per example.
                 // Maybe commands too?
                 let mut new_commands = job_def.commands.clone();
                 for cmd in &mut new_commands {
                      for (k, v) in &combo {
                          *cmd = cmd.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                      }
                 }

                 let mut req = create_base_job_request(job_def);
                 req.image = new_image;
                 req.commands = new_commands;
                 
                 // Append matrix values to env for convenience
                 let mut env = req.env.clone().unwrap_or_default();
                 for (k, v) in &combo {
                     env.insert(format!("MATRIX_{}", k.to_uppercase()), v.clone());
                     // Also substitute values into env vars
                     for (_, env_val) in env.iter_mut() {
                         *env_val = env_val.replace(&format!("${{{{ matrix.{} }}}}", k), v);
                     }
                 }
                 req.env = Some(env);

                 requests.push(req);
             }
             return requests;
        }
    }

    vec![create_base_job_request(job_def)]
}

fn create_base_job_request(job_def: &WorkflowJob) -> JobRequest {
    JobRequest {
        image: job_def.image.clone(),
        commands: job_def.commands.clone(),
        env: job_def.env.clone(),
        limits: None, // Could parse from YAML if we added fields
        callback_url: None,
        timeout_seconds: None,
        artifacts_path: None,
        user: None,
        cache_key: None,
        cache_paths: None,
        services: job_def.services.clone(),
        on_success: vec![],
        debug: false,
        priority: None,
        enable_network: false, // Default false, could add to YAML
        run_at: None,
    }
}

// Recursive function to build the tree
fn build_job_hierarchy(
    current_job_name: &str,
    job_map: &HashMap<String, Vec<JobRequest>>,
    dependents_map: &HashMap<String, Vec<String>>,
    visited_recursion: &mut std::collections::HashSet<String>,
    visited_global: &mut std::collections::HashSet<String>,
) -> Result<Vec<JobRequest>, String> {
    if visited_recursion.contains(current_job_name) {
        return Err(format!("Circular dependency detected involving job '{}'", current_job_name));
    }
    visited_recursion.insert(current_job_name.to_string());
    visited_global.insert(current_job_name.to_string());

    // Get the base jobs for this name (e.g. 3 matrix variants)
    let base_jobs = job_map.get(current_job_name)
        .ok_or_else(|| format!("Job {} not found in map", current_job_name))?;

    // Clone them so we can modify on_success
    let mut current_jobs = base_jobs.clone();

    // Find who depends on this job
    if let Some(dependent_names) = dependents_map.get(current_job_name) {
        for dep_name in dependent_names {
            // Recursively build the dependent jobs (and their descendants)
            let children_jobs = build_job_hierarchy(dep_name, job_map, dependents_map, visited_recursion, visited_global)?;
            
            // Attach these children to *each* of the current jobs
            // This is the Fan-out logic.
            for job in &mut current_jobs {
                job.on_success.extend(children_jobs.clone());
            }
        }
    }

    visited_recursion.remove(current_job_name);
    Ok(current_jobs)
}
