use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::http::StatusCode;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    CreateQueueRequest, CreateTaskRequest, DeleteTaskRequest, HttpRequest, ListQueuesRequest,
    Queue, Task, task,
};
use prost_types::Timestamp;
use tokio::net::TcpListener;
use tokio::sync::Notify;

#[tokio::test]
async fn reset_keeps_only_startup_queues() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(cloud_tasks_emulator::serve(
        listener,
        url.clone(),
        vec!["projects/p/locations/l/queues/a".to_owned()],
    ));
    let mut client = CloudTasksClient::connect(url.clone()).await.unwrap();
    client
        .create_queue(CreateQueueRequest {
            parent: "projects/p/locations/l".to_owned(),
            queue: Some(Queue {
                name: "projects/p/locations/l/queues/b".to_owned(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();

    let response = reqwest::Client::new()
        .post(format!("{url}/emulator:reset"))
        .send()
        .await
        .unwrap();

    let queues = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .queues;
    assert_eq!(
        (
            response.status(),
            queues
                .iter()
                .map(|queue| queue.name.as_str())
                .collect::<Vec<_>>()
        ),
        (
            StatusCode::NO_CONTENT,
            vec!["projects/p/locations/l/queues/a"]
        )
    );
}

#[tokio::test]
async fn reset_frees_task_names() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(cloud_tasks_emulator::serve(
        listener,
        url.clone(),
        vec!["projects/p/locations/l/queues/q".to_owned()],
    ));
    let mut client = CloudTasksClient::connect(url.clone()).await.unwrap();
    let request = CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
            message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                url: "http://127.0.0.1:1/t".to_owned(),
                ..Default::default()
            })),
            schedule_time: Some(Timestamp::from(
                SystemTime::now() + Duration::from_secs(24 * 60 * 60),
            )),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    };
    client.create_task(request.clone()).await.unwrap();
    client
        .delete_task(DeleteTaskRequest {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
        })
        .await
        .unwrap();

    reqwest::Client::new()
        .post(format!("{url}/emulator:reset"))
        .send()
        .await
        .unwrap();

    let task = client.create_task(request).await.unwrap().into_inner();
    assert_eq!(task.name, "projects/p/locations/l/queues/q/tasks/t");
}

#[tokio::test]
async fn reset_drops_attempts_in_flight() {
    let arrived = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = {
        let (arrived, release) = (arrived.clone(), release.clone());
        Router::new().fallback(move || async move {
            arrived.notify_one();
            release.notified().await;
            StatusCode::OK
        })
    };
    let handler_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let handler_url = format!("http://{}", handler_listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(handler_listener, handler).await.unwrap() });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(cloud_tasks_emulator::serve(
        listener,
        url.clone(),
        vec!["projects/p/locations/l/queues/q".to_owned()],
    ));
    let mut client = CloudTasksClient::connect(url.clone()).await.unwrap();
    client
        .create_task(CreateTaskRequest {
            parent: "projects/p/locations/l/queues/q".to_owned(),
            task: Some(Task {
                message_type: Some(task::MessageType::HttpRequest(HttpRequest {
                    url: format!("{handler_url}/t"),
                    ..Default::default()
                })),
                ..Default::default()
            }),
            response_view: task::View::Unspecified.into(),
        })
        .await
        .unwrap();
    arrived.notified().await;

    reqwest::Client::new()
        .post(format!("{url}/emulator:reset"))
        .send()
        .await
        .unwrap();
    release.notify_one();
    // Lets attempt from before reset finish
    tokio::time::sleep(Duration::from_millis(100)).await;

    let listed = client
        .list_queues(ListQueuesRequest {
            parent: "projects/p/locations/l".to_owned(),
            ..Default::default()
        })
        .await;
    assert!(listed.is_ok());
}
