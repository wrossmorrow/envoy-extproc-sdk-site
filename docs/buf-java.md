# Draft: routing the Java demo extproc through buf

> **Status: implemented.** Companion to `docs/buf-rust.md` (Rust) and the existing
> Go-side buf setup. Living document. Not yet reviewed.

## Background

The demo keeps a single source of truth for its API definitions: the `proto/`
module (`controller.v1`). Two code paths exist today:

- **Go:** `buf generate` (via `buf.gen.yaml`, managed mode, Go plugins →
  `gen/go`) is the codegen entry point. `just gen` runs `buf generate`.
- **Rust (see docs/buf-rust.md):** buf invokes a local
  `protoc-gen-tonic-prost` plugin (`buf.gen.yaml`: `local: ["protoc-gen-tonic-prost"]`,
  `out: gen/rust`).
- **Java (implemented):** buf generates the envoy ext_proc controller service
  gRPC client/server code via a local `protoc-gen-grpc-java` plugin and the
  protobuf runtime via buf's built-in `protoc_builtin: java` plugin, emitting
  into `gen/java`.

## Goal

- `buf generate` (i.e. `just gen`) produces Go, Rust and Java artifacts from the
  same `proto/` module.
- Buf is the single codegen orchestrator and source of truth for the demo's
  protobuf surface for the Java side, mirroring the existing Go and Rust setup.

## Implementation notes

### Plugins (`buf.gen.yaml`)

```yaml
  - local: ["protoc-gen-grpc-java"]
    out: gen/java
  - protoc_builtin: java
    out: gen/java
    protoc_path: protoc
```

- `protoc-gen-grpc-java` emits the gRPC service stubs (client + server) for
  package `com.controller.v1`, alongside the message types the stubs reference.
- The envoy ext_proc SDK (and the envoy gencode it ships) are built against
  **protobuf 3.x**, so the Java **message** generator must run with a **protobuf
  3.x protoc** binary. buf's default builtin java generator targets protobuf
  **4.x**, which produces gencode that fails at runtime with
  `VerifyError: Bad return type ... MapFieldReflectionAccessor ... current frame
  is not assignable to 'com.google.protobuf.MapFieldReflectionAccessor'`. The
  `protoc_path: protoc` entry points at a protobuf 3.x `protoc` binary on `PATH`
  (e.g. `protoc` 3.21.12, as provisioned locally at `/tmp/protoc-3.21.12`).

### Generated artifact layout

- `gen/java/com/controller/v1/*` — package `com.controller.v1` (the
  `controller.v1` proto package name), gRPC 1.x stubs matching the envoy ext_proc
  SDK's expectations, protobuf 3.x message classes.

### Gradle side

The Java demo extproc Gradle build (`java/demo-extproc/`) consumes the
generated code via `sourceSets` pointing at the repo-level `gen/java`
directory, and pins the runtime dependency to
`com.google.protobuf:protobuf-java:3.24.0` so the generated protobuf-3 gencode
and the SDK's bundled protobuf version do not conflict.

## Verification plan

- `just gen` produces the Java artifacts from `proto/`.
- `just java-build` and `just java-test` in `java/demo-extproc` pass; the
  generated code compiles against the SDK and the pinned protobuf runtime.

## Open questions

- Whether the protobuf 3.x protoc requirement should be pinned via a
  `protoc-gen-grpc-java`-style wrapper or a buf-managed plugin distribution.
- Whether the CI Proto Sync Check should cover the Java artifact alongside Go
  and Rust.
