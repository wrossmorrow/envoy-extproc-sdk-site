# Draft: routing the Rust demo extproc through buf

> **Status: draft, now partially implemented.** Living document. The buf-driven Rust codegen
> described here has been prototyped via a local `protoc-gen-tonic-prost` plugin and validated
> locally; the FileDescriptorSet-based mechanism below was the earlier proposal and the
> validated fallback. Not yet reviewed.
> Companion to the existing buf setup used for the Go side of the demo.

## Background

The demo keeps a single source of truth for its API definitions: the `proto/`
module (`controller.v1`). Two code paths exist today:

- **Go:** `buf generate` (via `buf.gen.yaml`, managed mode, Go plugins →
  `gen/go`) is the codegen entry point. `just gen` runs `buf generate`.
- **Rust (implemented):** `buf generate` invokes a local `protoc-gen-tonic-prost` plugin
  (`buf.gen.yaml`: `local: ["protoc-gen-tonic-prost"]`, `out: gen/rust`) that emits
  `gen/rust/controller.v1.rs` from the same `proto/` module. The crate includes that file via
  `include!("../../../gen/rust/controller.v1.rs")`; the direct
  `tonic_prost_build::configure().compile_protos(...)` call in `rust/demo-extproc/build.rs` is
  gone.

The Rust path no longer invokes protoc/tonic-prost-build against the `proto/` tree directly;
buf drives its codegen. Buf therefore lints, breaks, formats and generates Go and Rust through
the same plugin machinery.

## Goal

- `buf generate` (i.e. `just gen`) produces Go **and** Rust code from the same `proto/` module.
- Buf is the single codegen orchestrator and source of truth for the demo's protobuf surface.
- The Rust build no longer invokes protoc/tonic-prost-build against `proto/` directly; it
  consumes buf-generated output.
- The Go and Rust generated code stay in sync with `proto/` automatically, via the existing
  buf workflow (`buf lint`, `buf breaking`, `buf format`, and the CI Proto Sync Check).

## Proposed design (draft)

### Chosen mechanism: buf invokes a local `protoc-gen-tonic-prost` plugin

**Why this direction:** the Rust codegen stack (prost / tonic-prost / tonic-build) does not
ship a standalone `protoc-gen-*` plugin binary. Its generation entry points are the
prost-build/tonic-prost-build APIs. Buf drives codegen by invoking protoc plugins, so the
direct way to get Rust codegen "through buf" is to have buf invoke a local Rust plugin that
wraps the tonic-prost service generator; the plugin reuses the same public
`prost_build::Config::generate` service-generator machinery that `tonic_prost_build::configure()`
drives during a normal Cargo build. (The FileDescriptorSet-based mechanism below was the
earlier proposal and the locally validated fallback.)

**Buf side (implemented):**

```yaml
  - local: ["protoc-gen-tonic-prost"]
    out: gen/rust
```

The plugin emits a single generated file per proto package, named after the package with the
`.rs` extension (`controller.v1.rs` for package `controller.v1`), which lands at
`gen/rust/controller.v1.rs`.

**Rust side (implemented):**

`rust/demo-extproc/build.rs` no longer compiles protos; the generated file is consumed via
`include!` from the checked-in location:

```rust
pub mod controller {
    pub mod v1 {
        include!("../../../gen/rust/controller.v1.rs");
    }
}
```

The plugin (`rust/protoc-gen-tonic-prost`) reads a `CodeGeneratorRequest` from stdin, maps
each file descriptor to a module via `prost_build::Module::from_protobuf_package_name`, and
calls `prost_build::Config::generate` (registering the tonic service generator via
`tonic_prost_build::configure().service_generator()` and applying
`prost_build::Config::type_attribute(".", "#[allow(clippy::all)]")`). It writes a
`CodeGeneratorResponse` to stdout, declaring support for the `proto3 optional` feature.

**Validation (locally):** the plugin-generated file is byte-identical to the file previously
produced by `tonic_prost_build::configure().compile_protos`, and the crate builds and passes
its unit tests (8 passed) against it. `cargo fmt --check`, `cargo clippy --all-targets -- -D
warnings`, and `cargo build` are clean for the plugin and the demo crate.

### Alternative mechanism (draft): buf emits a FileDescriptorSet; Rust generates from it

An alternative is to have buf emit a **FileDescriptorSet** and have the Rust build script
generate Rust from that FDS via the existing public API:

- `tonic_prost_build::configure().compile_fds(fds)` (tonic-prost-build), or
- `prost_build::Config::new().file_descriptor_set_path(..).skip_protoc_run(true).compile_protos(..)`
  then `prost_build::Config::compile_fds(fds)`.

This was the earlier proposal for the Rust path, and the FDS-based generation was locally
validated (the generated module/naming surface is equivalent to the compiled-proto path; the
remaining step was replacing the locally-produced FDS with the one emitted by buf via a
descriptor plugin entry in `buf.gen.yaml`).

## Concrete changes proposed (now applied)

1. `buf.gen.yaml`: add the Rust plugin entry (`local: ["protoc-gen-tonic-prost"]`, `out: gen/rust`).
2. Rust build input: replace `tonic_prost_build::configure().compile_protos(...)` with the
   buf-generated file (build script removed; `include!` switched).
3. `justfile`: keep `gen: buf generate` as the single entry point; no change required if buf
   emits both Go and Rust artifacts. Rust recipes (`rust-build`, `rust-test`, `rust-fmt`,
   `rust-lint`) were added to support local checks.
4. CI (`.github/workflows/codegen.yaml`): extend the Proto Sync Check so the Rust artifact
   (`gen/rust/`) is regenerated and verified alongside `gen/go`. Proposed; not yet applied.

## Verification plan (draft)

- `just gen` produces Go and Rust artifacts from `proto/`.
- `just rust-build` and `just rust-test` in `rust/demo-extproc` pass with the plugin-generated
  file.
- `buf lint`, `buf breaking`, `buf format` clean on `proto/`.
- CI Proto Sync Check passes for both Go and Rust artifacts.

## Open questions

- **FDS emission mechanism:** no longer needed for the implemented path; kept as the validated
  fallback.
- **Generated Rust artifact layout:** now checked in under `gen/rust/` (via the buf plugin
  `out:`), mirroring `gen/go`; pending maintainer preference.
- **CI:** whether and how the sync check should cover Rust artifacts; not changed yet.
- **Behavior parity:** the plugin-generated code must reproduce the exact generated
  module/naming surface the processor code depends on (file names, module nesting, client/server
  trait shapes). Verified locally for the current protos via the plugin path; must be re-verified
  after any proto changes.
