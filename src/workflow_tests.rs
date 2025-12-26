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

        let jobs = parse_workflow(yaml).unwrap();
        assert_eq!(jobs.len(), 1); // Only 'build' is a root job
        
        let build_job = &jobs[0];
        assert_eq!(build_job.image, "rust:latest");
        assert_eq!(build_job.on_success.len(), 1);

        let test_job = &build_job.on_success[0];
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

        let jobs = parse_workflow(yaml).unwrap();
        assert_eq!(jobs.len(), 1);
        
        let build_job = &jobs[0];
        assert_eq!(build_job.on_success.len(), 4); // 2 versions * 2 OS = 4 jobs

        // Verify expansion
        let images: Vec<String> = build_job.on_success.iter().map(|j| j.image.clone()).collect();
        assert!(images.contains(&"test:1.0-linux".to_string()));
        assert!(images.contains(&"test:1.0-windows".to_string()));
        assert!(images.contains(&"test:2.0-linux".to_string()));
        assert!(images.contains(&"test:2.0-windows".to_string()));

        // Verify env vars
        let envs: Vec<_> = build_job.on_success.iter().map(|j| j.env.clone().unwrap()).collect();
        let linux_env = envs.iter().find(|e| e.get("MATRIX_OS").map(|s| s.as_str()) == Some("linux")).unwrap();
        assert_eq!(linux_env.get("MATRIX_OS"), Some(&"linux".to_string()));
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
        let jobs = parse_workflow(yaml).unwrap();
        assert_eq!(jobs.len(), 2);
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
        assert!(result.unwrap_err().contains("Circular dependency"));
    }
}
