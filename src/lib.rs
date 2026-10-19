mod api;
mod names;
mod scheduler;
mod state;

use std::sync::Arc;

use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

use crate::proto::google::cloud::tasks::v2::cloud_tasks_server::CloudTasksServer;

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

pub async fn serve(listener: TcpListener) -> Result<(), tonic::transport::Error> {
    let emulator = Arc::new(state::Emulator::default());
    tokio::spawn(scheduler::run(emulator.clone()));
    tonic::transport::Server::builder()
        .add_service(CloudTasksServer::new(api::CloudTasksService::new(emulator)))
        .serve_with_incoming(TcpIncoming::from(listener))
        .await
}
