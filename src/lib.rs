mod api;
mod filter;
mod names;
mod oidc;
mod rest;
mod scheduler;
mod state;

use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use prost_reflect::DescriptorPool;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tonic::service::Routes;
use tonic::transport::server::TcpIncoming;

use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::{CloudTasks, CloudTasksServer};
use crate::proto::google::cloud::tasks::v2::{CreateQueueRequest, Queue};
use crate::proto::google::longrunning::operations_server::OperationsServer;
use crate::state::{Emulator, QueueState};

pub mod proto {
    pub mod google {
        pub mod api {
            tonic::include_proto!("google.api");
        }
        pub mod cloud {
            pub mod tasks {
                pub mod v2 {
                    tonic::include_proto!("google.cloud.tasks.v2");
                }
            }
        }
        pub mod iam {
            pub mod v1 {
                tonic::include_proto!("google.iam.v1");
            }
        }
        pub mod longrunning {
            tonic::include_proto!("google.longrunning");
        }
        pub mod rpc {
            tonic::include_proto!("google.rpc");
        }
        pub mod r#type {
            tonic::include_proto!("google.r#type");
        }
    }
}

static DESCRIPTORS: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(include_bytes!(concat!(env!("OUT_DIR"), "/descriptors.bin")).as_slice())
        .expect("build.rs encodes the descriptors")
});

/// Serves gRPC, REST and OIDC discovery for `issuer`, creating each of `queues` with default config
pub async fn serve(
    listener: TcpListener,
    issuer: String,
    queues: Vec<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let emulator = Arc::new(Emulator {
        state: Mutex::new(startup_state(&queues).await?),
        wake: Notify::new(),
    });
    let issuer = Arc::new(oidc::Issuer::new(issuer));
    tokio::spawn(scheduler::run(emulator.clone(), issuer.clone()));
    let discovery = axum::Router::new()
        .route("/.well-known/openid-configuration", get(oidc::discovery))
        .route("/jwks", get(oidc::jwks))
        .with_state(issuer);
    let idle = axum::Router::new()
        .route("/emulator/{*path}", get(wait_idle))
        .with_state(emulator.clone());
    let reset = axum::Router::new()
        .route("/emulator:reset", post(reset))
        .with_state((emulator.clone(), Arc::new(queues)));
    let grpc = Routes::new(CloudTasksServer::from_arc(emulator.clone()))
        .add_service(OperationsServer::from_arc(emulator));
    let routes = grpc
        .clone()
        .into_axum_router()
        .merge(discovery)
        .merge(idle)
        .merge(reset)
        .merge(rest::router(grpc));
    tonic::transport::Server::builder()
        .accept_http1(true)
        .add_routes(routes.into())
        .serve_with_incoming(TcpIncoming::from(listener).with_nodelay(Some(true)))
        .await?;
    Ok(())
}

/// State holding only `queues`, each created with default config
async fn startup_state(queues: &[String]) -> Result<state::State, String> {
    let emulator = Emulator::default();
    for name in queues {
        let request = CreateQueueRequest {
            parent: name
                .rsplit_once("/queues/")
                .map_or(name.as_str(), |(parent, _)| parent)
                .to_owned(),
            queue: Some(Queue {
                name: name.clone(),
                ..Default::default()
            }),
        };
        emulator
            .create_queue(tonic::Request::new(request))
            .await
            .map_err(|status| format!("Cannot create {name}: {}", status.message()))?;
    }
    Ok(emulator.state.into_inner().unwrap())
}

/// Returns emulator to state it started with, keeping OIDC key
async fn reset(State((emulator, queues)): State<(Arc<Emulator>, Arc<Vec<String>>)>) -> StatusCode {
    let fresh = startup_state(&queues)
        .await
        .expect("startup created these queues already");
    emulator.state.lock().unwrap().reset(fresh);
    StatusCode::NO_CONTENT
}

/// Answers once queue in path such as `projects/p/locations/l/queues/q:waitIdle` holds no tasks
async fn wait_idle(State(emulator): State<Arc<Emulator>>, Path(path): Path<String>) -> Response {
    let Some(name) = path.strip_suffix(":waitIdle") else {
        return StatusCode::NOT_FOUND.into_response();
    };
    loop {
        {
            let state = emulator.state.lock().unwrap();
            let Some(queue) = state.queues.get(name) else {
                return (
                    StatusCode::NOT_FOUND,
                    format!("Queue {name} does not exist"),
                )
                    .into_response();
            };
            if queue.tasks().is_empty() {
                return StatusCode::NO_CONTENT.into_response();
            }
            if queue.state == QueueState::Paused {
                return (
                    StatusCode::CONFLICT,
                    format!("Queue {name} is paused with tasks left"),
                )
                    .into_response();
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
