#[cfg(test)]
mod tests {
    use crate::workflow::parse_workflow;

    #[test]
    fn test_parse_simple_chain() {
        let yaml = r#"
name: Simple Build
jobs:
  build:
    image: rust:latest
    commands: ["cargo build"]
  
  test:
    needs: [build]
    image: rust:latest
    commands: ["cargo test"]
"#;

        let graph = parse_workflow(yaml).unwrap();
        
        // build and test
        assert_eq!(graph.jobs.len(), 2);
        
        let build_job = graph.jobs.get("build").expect("build job missing");
        assert_eq!(build_job.image, "rust:latest");
        
        // build should have 'test' as dependency in the dependency map
        // Dependencies map is Parent -> [Children]
        let children = graph.dependencies.get("build").expect("build should have dependencies");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], "test");

        let test_job = graph.jobs.get("test").expect("test job missing");
        assert_eq!(test_job.commands, vec!["cargo test"]);
    }

    #[test]
    fn test_matrix_fan_out() {
        let yaml = r#"
name: Matrix Build
jobs:
  build:
    image: base:latest
    commands: ["prepare"]

  test:
    needs: [build]
    strategy:
      matrix:
        version: ["1.0", "2.0"]
        os: ["linux", "windows"]
    image: test:${{ matrix.version }}-${{ matrix.os }}
    commands: ["run test ${{ matrix.version }}"]
"#;

        let graph = parse_workflow(yaml).unwrap();
        
        // 1 build job + 4 test jobs = 5 total
        assert_eq!(graph.jobs.len(), 5);
        
        // Check dependencies: Build -> 4 test jobs
        let children = graph.dependencies.get("build").expect("build should have children");
        assert_eq!(children.len(), 4);

        let images: Vec<String> = children.iter()
            .map(|key| graph.jobs.get(key).unwrap().image.clone())
            .collect();
            
        assert!(images.contains(&"test:1.0-linux".to_string()));
        assert!(images.contains(&"test:1.0-windows".to_string()));
        assert!(images.contains(&"test:2.0-linux".to_string()));
        assert!(images.contains(&"test:2.0-windows".to_string()));

        // Check env vars for one instance
        // Keys are likely "test-1.0-linux" etc based on my implementation sorting
        // But exact key string depends on suffix generation
        // Let's find one
        let key = children.iter().find(|k| graph.jobs.get(*k).unwrap().image == "test:1.0-linux").unwrap();
        let job = graph.jobs.get(key).unwrap();
        let env = job.env.as_ref().unwrap();
        assert_eq!(env.get("MATRIX_OS"), Some(&"linux".to_string()));
    }

    #[test]
    fn test_independent_jobs() {
        let yaml = r#"
name: Parallel Jobs
jobs:
  job1:
    image: img1
    commands: ["cmd1"]
  job2:
    image: img2
    commands: ["cmd2"]
"#;
        let graph = parse_workflow(yaml).unwrap();
        assert_eq!(graph.jobs.len(), 2);
        assert!(graph.dependencies.is_empty());
    }

    #[test]
    fn test_circular_dependency() {
        let yaml = r#"
name: Circular
jobs:
  job1:
    image: img1
    commands: ["cmd1"]
    needs: [job2]
  job2:
    image: img2
    commands: ["cmd2"]
    needs: [job1]
"#;
        let result = parse_workflow(yaml);
        assert!(result.is_err());
        match result {
            Err(crate::workflow::WorkflowError::CycleDetected(node)) => {
                assert!(node == "job1" || node == "job2");
            },
            _ => panic!("Expected CycleDetected error"),
        }
    }
}
