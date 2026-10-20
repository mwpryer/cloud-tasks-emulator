mod support;

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    CmekConfig, CreateQueueRequest, GetCmekConfigRequest, HttpTarget, Queue,
    UpdateCmekConfigRequest, UpdateQueueRequest, http_target,
};
use tonic::Code;

fn turn_on(location: &str) -> UpdateCmekConfigRequest {
    set_key(location, &format!("{location}/keyRings/r/cryptoKeys/k"))
}

fn set_key(location: &str, kms_key: &str) -> UpdateCmekConfigRequest {
    UpdateCmekConfigRequest {
        cmek_config: Some(CmekConfig {
            name: format!("{location}/cmekConfig"),
            kms_key: kms_key.to_owned(),
        }),
        update_mask: None,
    }
}

fn with_header_override(name: &str) -> Queue {
    Queue {
        name: name.to_owned(),
        http_target: Some(HttpTarget {
            header_overrides: vec![http_target::HeaderOverride {
                header: Some(http_target::Header {
                    key: "X-Custom".to_owned(),
                    value: "value".to_owned(),
                }),
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn get_cmek_config_returns_key_set() {
    let mut client = support::start_emulator().await;
    client
        .update_cmek_config(turn_on("projects/p/locations/l"))
        .await
        .unwrap();

    let config = client
        .get_cmek_config(GetCmekConfigRequest {
            name: "projects/p/locations/l/cmekConfig".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        config,
        CmekConfig {
            name: "projects/p/locations/l/cmekConfig".to_owned(),
            kms_key: "projects/p/locations/l/keyRings/r/cryptoKeys/k".to_owned(),
        }
    );
}

#[tokio::test]
async fn update_cmek_config_with_empty_key_turns_cmek_off() {
    let mut client = support::start_emulator().await;
    client
        .update_cmek_config(turn_on("projects/p/locations/l"))
        .await
        .unwrap();
    client
        .update_cmek_config(set_key("projects/p/locations/l", ""))
        .await
        .unwrap();

    let config = client
        .get_cmek_config(GetCmekConfigRequest {
            name: "projects/p/locations/l/cmekConfig".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(config.kms_key, "");
}

#[tokio::test]
async fn create_queue_refuses_header_overrides_with_cmek_on() {
    let mut client = support::start_emulator().await;
    client
        .update_cmek_config(turn_on("projects/p/locations/l"))
        .await
        .unwrap();

    let status = client
        .create_queue(CreateQueueRequest {
            parent: "projects/p/locations/l".to_owned(),
            queue: Some(with_header_override("projects/p/locations/l/queues/q")),
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn update_queue_refuses_header_overrides_with_cmek_on() {
    let mut client = support::start_emulator().await;
    client
        .update_cmek_config(turn_on("projects/p/locations/l"))
        .await
        .unwrap();

    let status = client
        .update_queue(UpdateQueueRequest {
            queue: Some(with_header_override("projects/p/locations/l/queues/q")),
            update_mask: None,
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::FailedPrecondition);
}
