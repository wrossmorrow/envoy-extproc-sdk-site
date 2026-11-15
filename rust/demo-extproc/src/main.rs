use std::sync::Arc;

use demo_extproc::processor::DemoRequestProcessor;
use demo_extproc::{Error, GenericExtProcServer, Metrics, ProcessingOptions, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let processor = Arc::new(DemoRequestProcessor::new("controller:9093"));
    let server = GenericExtProcServer::new(
        "demo".to_string(),
        processor,
        Some(ProcessingOptions {
            abort_on_processor_failure: true,
            ..Default::default()
        }),
        Metrics::new(),
    );
    server.serve(ServerOptions::default()).await
}
