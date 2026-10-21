mod api;
mod filter;
mod names;
mod oidc;
mod rest;
mod scheduler;
mod state;

use std::sync::Arc;

use axum::routing::get;
use tokio::net::TcpListener;
use tonic::service::Routes;
use tonic::transport::server::TcpIncoming;

use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::CloudTasksServer;
use crate::proto::google::longrunning::operations_server::OperationsServer;

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

/// Serves the API over gRPC and REST, and OIDC discovery for tokens issued as `issuer`, an origin such as `http://127.0.0.1:8123`
/// App Engine tasks go to the `app_engine` origin, and without one every project lacks an App Engine app
pub async fn serve(
    listener: TcpListener,
    issuer: String,
    app_engine: Option<String>,
) -> Result<(), tonic::transport::Error> {
    let emulator = Arc::new(state::Emulator::new(app_engine));
    let issuer = Arc::new(oidc::Issuer::new(issuer));
    tokio::spawn(scheduler::run(emulator.clone(), issuer.clone()));
    let discovery = axum::Router::new()
        .route("/.well-known/openid-configuration", get(oidc::discovery))
        .route("/jwks", get(oidc::jwks))
        .with_state(issuer);
    let grpc = Routes::new(CloudTasksServer::new(api::CloudTasksService::new(
        emulator.clone(),
    )))
    .add_service(OperationsServer::new(api::OperationsService::new(emulator)));
    let routes = grpc
        .clone()
        .into_axum_router()
        .merge(discovery)
        .merge(rest::router(grpc));
    tonic::transport::Server::builder()
        .accept_http1(true)
        .add_routes(routes.into())
        .serve_with_incoming(TcpIncoming::from(listener))
        .await
}
