fn main() {
    let res = prost_build::compile_protos(
        &["../../proto/controller/v1/controller.proto"],
        &["../../proto"],
    );
    res.unwrap();
}
