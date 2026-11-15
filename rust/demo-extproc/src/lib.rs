pub mod controller {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/controller.v1.rs"));
    }
}

pub mod processor;

pub use envoy_extproc_sdk::context::{
    build_header_values_from_map, HeaderValue, RequestContext, RequestPhase,
};
pub use envoy_extproc_sdk::headers::AllHeaders;
pub use envoy_extproc_sdk::{
    Error, GenericExtProcServer, Metrics, ProcessingOptions, RequestProcessor, ServerOptions,
};
