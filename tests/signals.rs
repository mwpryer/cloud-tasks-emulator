use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

#[test]
fn sigterm_exits_cleanly() {
    let mut emulator = Command::new(env!("CARGO_BIN_EXE_cloud-tasks-emulator"))
        .env("LISTEN_ADDR", "127.0.0.1:0")
        .env("OIDC_ISSUER", "http://127.0.0.1:8123")
        .env("QUEUES", "")
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    BufReader::new(emulator.stderr.take().unwrap())
        .lines()
        .next()
        .unwrap()
        .unwrap();

    Command::new("kill")
        .args(["-TERM", &emulator.id().to_string()])
        .status()
        .unwrap();

    assert_eq!(emulator.wait().unwrap().code(), Some(0));
}
