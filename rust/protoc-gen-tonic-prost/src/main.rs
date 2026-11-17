//! protoc plugin that generates Rust gRPC code via the tonic/prost stack.
//!
//! Reads a `CodeGeneratorRequest` from stdin (encoded by protoc/buf) and writes a
//! `CodeGeneratorResponse` to stdout. Code generation reuses `prost_build::Config`'s
//! service-generator machinery (the same one `tonic_prost_build::configure()` drives under
//! the hood during a normal Cargo build), so the generated output is identical to the
//! one produced by `tonic_prost_build::configure().compile_protos(...)`.

use std::io::{Read, Write};

use prost::Message;
use prost_build::{Config, Module};
use prost_types::compiler::code_generator_response::{Feature as CodegenFeature, File};
use prost_types::compiler::{CodeGeneratorRequest, CodeGeneratorResponse};

fn main() {
    let mut input = Vec::new();
    std::io::stdin()
        .read_to_end(&mut input)
        .expect("failed to read CodeGeneratorRequest from stdin");
    let request = CodeGeneratorRequest::decode(&mut input.as_slice())
        .expect("failed to decode CodeGeneratorRequest");

    let mut config = Config::new();
    config.service_generator(tonic_prost_build::configure().service_generator());
    config.type_attribute(".", "#[allow(clippy::all)]");

    let mut requests = Vec::new();
    for file in request.proto_file {
        let module = Module::from_protobuf_package_name(file.package.as_deref().unwrap_or(""));
        requests.push((module, file));
    }

    let modules = config
        .generate(requests)
        .expect("failed to generate Rust code from CodeGeneratorRequest");

    let mut files = Vec::new();
    for (module, content) in modules {
        files.push(File {
            name: Some(module.to_file_name_or("_")),
            content: Some(content),
            insertion_point: None,
            generated_code_info: None,
        });
    }

    let response = CodeGeneratorResponse {
        error: None,
        supported_features: Some(CodegenFeature::Proto3Optional as u64),
        file: files,
    };
    let encoded = response.encode_to_vec();
    std::io::stdout()
        .write_all(&encoded)
        .expect("failed to write CodeGeneratorResponse to stdout");
}
