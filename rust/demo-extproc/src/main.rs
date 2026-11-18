use std::sync::Arc;

use demo_extproc::processor::DemoRequestProcessor;
use demo_extproc::{Error, GenericExtProcServer, Metrics, ProcessingOptions, ServerOptions};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // tonic endpoint URLs require a scheme (e.g. http://controller:9093); a bare
    // host such as `controller:9093` is rejected with "invalid URL, scheme is
    // missing", so the controller phase stream can never be opened.
    let processor = Arc::new(DemoRequestProcessor::new("http://controller:9093"));
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
