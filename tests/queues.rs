mod support;

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    AppEngineRouting, CreateQueueRequest, GetQueueRequest, ListQueuesRequest, Queue,
};
use tokio::net::TcpListener;
use tonic::Code;

fn create(parent: &str, queue: Queue) -> CreateQueueRequest {
    CreateQueueRequest {
        parent: parent.to_owned(),
        queue: Some(queue),
    }
}

fn named(name: &str) -> Queue {
    Queue {
        name: name.to_owned(),
        ..Default::default()
    }
}

#[tokio::test]
async fn get_queue_returns_created_queue() {
    let mut client = support::start_emulator().await;
    let created = client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/q"),
        ))
        .await
        .unwrap()
        .into_inner();

    let queue = client
        .get_queue(GetQueueRequest {
            name: "projects/p/locations/l/queues/q".to_owned(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(queue, created);
}

#[tokio::test]
async fn list_queues_returns_location_queues_in_name_order() {
    let mut client = support::start_emulator().await;
    client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/b"),
        ))
        .await
        .unwrap();
    client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/a"),
        ))
        .await
        .unwrap();
    client
        .create_queue(create(
            "projects/p/locations/other",
            named("projects/p/locations/other/queues/c"),
        ))
        .await
        .unwrap();

    let response = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response
            .queues
            .iter()
            .map(|queue| queue.name.as_str())
            .collect::<Vec<_>>(),
        [
            "projects/p/locations/l/queues/a",
            "projects/p/locations/l/queues/b"
        ]
    );
}

#[tokio::test]
async fn list_queues_pages_by_page_size() {
    let mut client = support::start_emulator().await;
    client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/a"),
        ))
        .await
        .unwrap();
    client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/b"),
        ))
        .await
        .unwrap();
    client
        .create_queue(create(
            "projects/p/locations/l",
            named("projects/p/locations/l/queues/c"),
        ))
        .await
        .unwrap();
    let first = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            page_size: 2,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let second = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            page_size: 2,
            page_token: first.next_page_token,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        (
            second
                .queues
                .iter()
                .map(|queue| queue.name.as_str())
                .collect::<Vec<_>>(),
            second.next_page_token.is_empty()
        ),
        (vec!["projects/p/locations/l/queues/c"], false)
    );
}

#[tokio::test]
async fn list_queues_rejects_malformed_filter() {
    let mut client = support::start_emulator().await;

    let status = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            filter: "state: (".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn routing_override_needs_app_engine_url() {
    let mut client = support::start_emulator().await;

    let status = client
        .create_queue(create(
            "projects/p/locations/l",
            Queue {
                app_engine_routing_override: Some(AppEngineRouting {
                    service: "worker".to_owned(),
                    ..Default::default()
                }),
                ..named("projects/p/locations/l/queues/q")
            },
        ))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn startup_creates_listed_queues() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(cloud_tasks_emulator::serve(
        listener,
        url.clone(),
        None,
        vec![
            "projects/p/locations/l/queues/b".to_owned(),
            "projects/p/locations/l/queues/a".to_owned(),
        ],
    ));
    let mut client = CloudTasksClient::connect(url).await.unwrap();

    let response = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response
            .queues
            .iter()
            .map(|queue| queue.name.as_str())
            .collect::<Vec<_>>(),
        [
            "projects/p/locations/l/queues/a",
            "projects/p/locations/l/queues/b"
        ]
    );
}

#[tokio::test]
async fn startup_fails_on_rejected_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let error = cloud_tasks_emulator::serve(
        listener,
        "http://127.0.0.1:8123".to_owned(),
        None,
        vec!["projects/p/locations/l/queues/a_b".to_owned()],
    )
    .await
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        r#"Cannot create projects/p/locations/l/queues/a_b: Queue ID "a_b" can contain only letters ([A-Za-z]), numbers ([0-9]), or hyphens (-). Queue ID must be between 1 and 100 characters."#
    );
}
