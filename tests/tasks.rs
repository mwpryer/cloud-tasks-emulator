mod support;

use std::time::{Duration, SystemTime};

use cloud_tasks_emulator::proto::google::cloud::tasks::v2::cloud_tasks_client::CloudTasksClient;
use cloud_tasks_emulator::proto::google::cloud::tasks::v2::{
    AppEngineHttpRequest, AppEngineRouting, CreateQueueRequest, CreateTaskRequest,
    DeleteTaskRequest, GetTaskRequest, HttpRequest, ListTasksRequest, Queue, Task, task,
};
use prost_types::Timestamp;
use tonic::Code;
use tonic::transport::Channel;

async fn start_with_queue() -> CloudTasksClient<Channel> {
    let mut client = support::start_emulator().await;
    client
        .create_queue(CreateQueueRequest {
            parent: "projects/p/locations/l".to_owned(),
            queue: Some(Queue {
                name: "projects/p/locations/l/queues/q".to_owned(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    client
}

/// A day out, so control-plane tests see no dispatch
fn tomorrow() -> Timestamp {
    Timestamp::from(SystemTime::now() + Duration::from_secs(24 * 60 * 60))
}

fn create(name: &str, http_request: HttpRequest) -> CreateTaskRequest {
    CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            name: name.to_owned(),
            message_type: Some(task::MessageType::HttpRequest(http_request)),
            schedule_time: Some(tomorrow()),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    }
}

fn http(url: &str) -> HttpRequest {
    HttpRequest {
        url: url.to_owned(),
        ..Default::default()
    }
}

#[tokio::test]
async fn list_tasks_returns_queue_tasks_in_name_order() {
    let mut client = start_with_queue().await;
    client
        .create_task(create(
            "projects/p/locations/l/queues/q/tasks/b",
            http("http://localhost/work"),
        ))
        .await
        .unwrap();
    client
        .create_task(create(
            "projects/p/locations/l/queues/q/tasks/a",
            http("http://localhost/work"),
        ))
        .await
        .unwrap();

    let response = client
        .list_tasks(ListTasksRequest {
            parent: "projects/p/locations/l/queues/q".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response
            .tasks
            .iter()
            .map(|task| task.name.as_str())
            .collect::<Vec<_>>(),
        [
            "projects/p/locations/l/queues/q/tasks/a",
            "projects/p/locations/l/queues/q/tasks/b"
        ]
    );
}

#[tokio::test]
async fn delete_task_removes_task() {
    let mut client = start_with_queue().await;
    client
        .create_task(create(
            "projects/p/locations/l/queues/q/tasks/t",
            http("http://localhost/work"),
        ))
        .await
        .unwrap();
    client
        .delete_task(DeleteTaskRequest {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
        })
        .await
        .unwrap();

    let status = client
        .get_task(GetTaskRequest {
            name: "projects/p/locations/l/queues/q/tasks/t".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::NotFound);
}

#[tokio::test]
async fn app_engine_host_ends_with_appspot_domain() {
    let (mut client, _) = support::start_emulator_with(Some("http://127.0.0.1:8080")).await;
    client
        .create_queue(CreateQueueRequest {
            parent: "projects/p/locations/l".to_owned(),
            queue: Some(Queue {
                name: "projects/p/locations/l/queues/q".to_owned(),
                ..Default::default()
            }),
        })
        .await
        .unwrap();
    let request = CreateTaskRequest {
        parent: "projects/p/locations/l/queues/q".to_owned(),
        task: Some(Task {
            message_type: Some(task::MessageType::AppEngineHttpRequest(
                AppEngineHttpRequest {
                    app_engine_routing: Some(AppEngineRouting {
                        service: "worker".to_owned(),
                        version: "v1".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
            schedule_time: Some(tomorrow()),
            ..Default::default()
        }),
        response_view: task::View::Unspecified.into(),
    };

    let task = client.create_task(request).await.unwrap().into_inner();

    let Some(task::MessageType::AppEngineHttpRequest(request)) = task.message_type else {
        panic!("{task:?} is an App Engine task");
    };
    assert_eq!(
        request.app_engine_routing.unwrap().host,
        "v1.worker.p.appspot.com"
    );
}
