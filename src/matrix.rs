use crate::models::JobRequest;
use itertools::Itertools;
use std::collections::HashMap;

/// Expands a JobRequest with a matrix into a list of JobRequests.
/// If no matrix is present, returns a list containing only the original request (cloned).
pub fn expand_job_request(request: &JobRequest) -> Vec<JobRequest> {
    if let Some(matrix) = &request.matrix {
        if matrix.is_empty() {
            return vec![request.clone()];
        }

        let keys: Vec<&String> = matrix.keys().collect();
        let values: Vec<&Vec<String>> = matrix.values().collect();

        // Calculate Cartesian product of all value lists
        let combinations = values.into_iter().multi_cartesian_product();

        let mut requests = Vec::new();

        for combination in combinations {
            let mut vars = HashMap::new();
            for (i, value) in combination.into_iter().enumerate() {
                vars.insert(keys[i].clone(), value.clone());
            }

            let mut new_req = request.clone();
            // Substitute variables in image, commands, and env
            substitute_variables(&mut new_req, &vars);
            
            // Clear the matrix in the expanded request to avoid infinite recursion if re-processed
            new_req.matrix = None;
            
            requests.push(new_req);
        }
        requests
    } else {
        vec![request.clone()]
    }
}

fn substitute_variables(req: &mut JobRequest, vars: &HashMap<String, String>) {
    req.image = substitute_string(&req.image, vars);
    
    req.commands = req.commands.iter()
        .map(|cmd| substitute_string(cmd, vars))
        .collect();
    
    if let Some(env) = &mut req.env {
        for val in env.values_mut() {
            *val = substitute_string(val, vars);
        }
		
        for (k, v) in vars {
            env.insert(k.clone(), v.clone());
        }
    } else {
        // If env was None, create it with matrix vars
        req.env = Some(vars.clone());
    }
}

fn substitute_string(input: &str, vars: &HashMap<String, String>) -> String {
    let mut result = input.to_string();
    for (key, value) in vars {
        // Replace ${key} with value
        let placeholder = format!("${{{}}}", key);
        result = result.replace(&placeholder, value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_no_matrix() {
        let req = JobRequest {
            image: "node:18".to_string(),
            commands: vec!["echo hello".to_string()],
            env: None,
            limits: None,
            callback_url: None,
            timeout_seconds: None,
            artifacts_path: None,
            user: None,
            cache_key: None,
            cache_paths: None,
            services: vec![],
            on_success: vec![],
            debug: false,
            priority: None,
            enable_network: false,
            run_at: None,
            matrix: None,
        };
        let expanded = expand_job_request(&req);
        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].image, "node:18");
    }

    #[test]
    fn test_expand_matrix() {
        let mut matrix = HashMap::new();
        matrix.insert("node".to_string(), vec!["18".to_string(), "20".to_string()]);
        matrix.insert("os".to_string(), vec!["alpine".to_string(), "debian".to_string()]);

        let req = JobRequest {
            image: "node:${node}-${os}".to_string(),
            commands: vec!["echo running on ${os}".to_string()],
            env: None,
            limits: None,
            callback_url: None,
            timeout_seconds: None,
            artifacts_path: None,
            user: None,
            cache_key: None,
            cache_paths: None,
            services: vec![],
            on_success: vec![],
            debug: false,
            priority: None,
            enable_network: false,
            run_at: None,
            matrix: Some(matrix),
        };

        let expanded = expand_job_request(&req);
        assert_eq!(expanded.len(), 4); // 2 * 2 = 4

        // Verify substitution
        let has_node18_alpine = expanded.iter().any(|r| r.image == "node:18-alpine" && r.commands[0] == "echo running on alpine");
        assert!(has_node18_alpine);
        
        // Verify env injection
        let r = expanded.first().unwrap();
        assert!(r.env.is_some());
        let env = r.env.as_ref().unwrap();
        assert!(env.contains_key("node"));
        assert!(env.contains_key("os"));
    }
}
