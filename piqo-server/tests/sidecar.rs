use std::{
    io::{BufRead, BufReader, Read},
    process::{Command, Stdio},
};

use serde_json::Value;
use tempfile::TempDir;

struct RunningSidecar {
    child: std::process::Child,
    ready: Value,
    stderr: Option<std::process::ChildStderr>,
    _home: TempDir,
}

impl RunningSidecar {
    fn start() -> Self {
        let home = tempfile::tempdir().expect("temporary HOME");
        let mut child = Command::new(env!("CARGO_BIN_EXE_piqo-server"))
            .env("HOME", home.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("sidecar starts");
        let stdout = child.stdout.take().expect("sidecar stdout");
        let mut lines = BufReader::new(stdout).lines();
        let ready: Value =
            serde_json::from_str(&lines.next().expect("ready line").expect("ready output"))
                .expect("ready JSON");
        assert_eq!(ready["type"], "ready");
        Self {
            stderr: child.stderr.take(),
            child,
            ready,
            _home: home,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.ready["base_url"].as_str().unwrap(), path)
    }

    fn token(&self) -> &str {
        self.ready["token"].as_str().unwrap()
    }

    fn stop(mut self) -> String {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as libc::pid_t, libc::SIGTERM);
        }
        #[cfg(windows)]
        self.child.kill().expect("sidecar kills");
        let status = self.child.wait().expect("sidecar waits");
        assert!(status.success(), "sidecar exit status: {status}");
        let mut stderr = String::new();
        self.stderr
            .take()
            .expect("sidecar stderr")
            .read_to_string(&mut stderr)
            .expect("stderr reads");
        stderr
    }
}

#[tokio::test]
async fn sidecar_announces_ephemeral_port_and_requires_token() {
    let sidecar = RunningSidecar::start();
    assert!(!sidecar.ready["base_url"].as_str().unwrap().ends_with(":0"));
    assert_eq!(sidecar.ready["protocol_version"], 1);
    assert_eq!(sidecar.ready["api_version"], "v1");
    assert_eq!(sidecar.token().len(), 43);

    let client = reqwest::Client::new();
    let response = client
        .get(sidecar.url("/api/v1/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);

    let response = client
        .get(sidecar.url("/api/v1/health"))
        .bearer_auth(sidecar.token())
        .send()
        .await
        .expect("authenticated health request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let health: Value = response.json().await.expect("health JSON");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["api_version"], "v1");

    let token = sidecar.token().to_owned();
    let profile = sidecar._home.path().join(".config/piqo");
    let lock = std::fs::read_to_string(profile.join("piqo.lock")).expect("lock reads");
    let database = std::fs::read(profile.join("piqo.db")).expect("database reads");
    assert!(!lock.contains(&token));
    assert!(!String::from_utf8_lossy(&database).contains(&token));
    let stderr = sidecar.stop();
    assert!(!stderr.contains(&token));
}

#[tokio::test]
async fn sidecar_rejects_a_second_instance_for_the_same_profile() {
    let first = RunningSidecar::start();
    let home = first._home.path().to_owned();
    let second = Command::new(env!("CARGO_BIN_EXE_piqo-server"))
        .env("HOME", home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("second sidecar starts");
    let output = second.wait_with_output().expect("second sidecar exits");
    assert_eq!(output.status.code(), Some(2));
    let fatal: Value = serde_json::from_slice(&output.stdout).expect("fatal JSON");
    assert_eq!(fatal["type"], "fatal");
    assert_eq!(fatal["code"], "instance_already_running");
    let _ = first.stop();
}

#[test]
fn sidecar_reports_a_versioned_fatal_message_without_home() {
    let output = Command::new(env!("CARGO_BIN_EXE_piqo-server"))
        .env_remove("HOME")
        .output()
        .expect("sidecar starts");
    assert_eq!(output.status.code(), Some(2));
    let fatal: Value = serde_json::from_slice(&output.stdout).expect("fatal JSON");
    assert_eq!(fatal["type"], "fatal");
    assert_eq!(fatal["protocol_version"], 1);
    assert_eq!(fatal["code"], "home_unavailable");
}
