
#[cfg(test)]
mod tests {
    use crate::models::JobRequest;
    use crate::matrix;
    use std::collections::HashMap;

    #[test]
    fn test_matrix_expansion_and_substitution() {
        let mut matrix = HashMap::new();
        matrix.insert("node".to_string(), vec!["18".to_string(), "20".to_string()]);
        matrix.insert("os".to_string(), vec!["alpine".to_string(), "ubuntu".to_string()]);

        let req = JobRequest {
            image: "node:${node}-${os}".to_string(),
            commands: vec!["echo running on ${os} with node ${node}".to_string()],
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

        let expanded = matrix::expand_job_request(&req);

        // Expect 2x2 = 4 jobs
        assert_eq!(expanded.len(), 4);

        // Check for specific combinations
        let has_node18_alpine = expanded.iter().any(|j| 
            j.image == "node:18-alpine" && 
            j.commands[0] == "echo running on alpine with node 18" &&
            j.env.as_ref().unwrap().get("node").unwrap() == "18" &&
            j.env.as_ref().unwrap().get("os").unwrap() == "alpine"
        );
        assert!(has_node18_alpine, "Missing node 18 alpine combination");

        let has_node20_ubuntu = expanded.iter().any(|j| 
            j.image == "node:20-ubuntu" && 
            j.commands[0] == "echo running on ubuntu with node 20"
        );
        assert!(has_node20_ubuntu, "Missing node 20 ubuntu combination");
        
        // Ensure matrix field is cleared to prevent re-expansion
        for job in expanded {
            assert!(job.matrix.is_none());
        }
    }

    #[test]
    fn test_no_matrix() {
        let req = JobRequest {
            image: "simple:latest".to_string(),
            commands: vec!["ls".to_string()],
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

        let expanded = matrix::expand_job_request(&req);
        assert_eq!(expanded.len(), 1);
        assert_eq!(expanded[0].image, "simple:latest");
        assert!(expanded[0].matrix.is_none());
    }
}
