//! Interactive demo extproc processor.

use std::sync::Arc;
use std::time::Duration;

use envoy_extproc_sdk::{
    build_header_values_from_map, AllHeaders, Error, GenericExtProcServer, HeaderAppendAction,
    HeaderValue, is_stream_cancelled, Metrics, ProcessingOptions, RequestContext, RequestPhase,
    RequestProcessor, ServerOptions,
};
use envoy_types::pb::controller::v1::decision::Action;
use envoy_types::pb::controller::v1::{Cancel, Continue, Decision, Header, HttpSnapshot, ProcessRequest, ProcessResponse};
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingResponse;
use envoy_types::pb::envoy::service::ext_proc::v3::ProcessingRequest;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;


const SESSION_ID_HEADER: &str = "x-demo-session-id";
const PROCESSED_HEADER: &str = "x-demo-processed";
const SNAPSHOT_BODY_LIMIT: usize = 16 * 1024;
const DEFAULT_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Default)]
pub struct ControllerExchange {
    pub stream: Option<ControllerStream>,
}

#[derive(Clone)]
pub struct ControllerStream {
    pub tx: tokio::sync::mpsc::UnboundedSender<ProcessRequest>,
    pub response: std::sync::Arc<tokio::sync::Mutex<tonic::codec::Streaming<ProcessResponse>>>,
}

pub struct DemoRequestProcessor {
    controller_addr: String,
    options: ProcessingOptions,
    controller: Arc<tokio::sync::Mutex<Option<tonic::client::Grpc<tonic::transport::Channel>>>>,
}

impl DemoRequestProcessor {
    pub fn new(controller_addr: &str) -> Self {
        Self {
            controller_addr: controller_addr.to_string(),
            options: ProcessingOptions {
                abort_on_processor_failure: true,
                ..Default::default()
            },
            controller: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    fn options(&self) -> &ProcessingOptions {
        &self.options
    }

    async fn controller_stream(
        &self,
    ) -> Result<(tokio::sync::mpsc::UnboundedSender<ProcessRequest>, tonic::codec::Streaming<ProcessResponse>), Error> {
        let mut guard = self.controller.lock().await;
        let channel = guard
            .as_ref()
            .map(|g| g.clone())
            .map(|_| tonic::transport::Channel::default())
            .unwrap_or_else(|| {
                tonic::transport::Channel::default()
            });
        let _ = channel;
        unreachable!()
    }
}
