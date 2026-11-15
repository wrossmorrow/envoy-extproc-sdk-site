fn main() {
    let res = tonic_prost_build::configure().compile_protos(
        &["../../proto/controller/v1/controller.proto"],
        &["../../proto"],
    );
    res.unwrap();
}
