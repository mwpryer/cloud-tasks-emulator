mod support;

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    CreateQueueRequest, GetQueueRequest, ListQueuesRequest, Queue,
};
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
