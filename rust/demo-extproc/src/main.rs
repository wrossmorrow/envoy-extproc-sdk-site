use std::sync::Arc;

use demo_extproc::processor::DemoRequestProcessor;
use demo_extproc::{Error, GenericExtProcServer, Metrics, ProcessingOptions, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Controller gRPC endpoint for the phase exchange, from the CONTROLLER_ADDR
    // environment variable (default http://controller:9093; tonic endpoint URLs
    // require a scheme).
    let processor = Arc::new(DemoRequestProcessor::from_env());
    let server = GenericExtProcServer::new(
        "demo".to_string(),
        processor,
        Some(ProcessingOptions {
            abort_on_processor_failure: DemoRequestProcessor::abort_on_processor_failure(),
            ..Default::default()
        }),
        Metrics::new(),
    );
    server.serve(ServerOptions::default()).await
}
