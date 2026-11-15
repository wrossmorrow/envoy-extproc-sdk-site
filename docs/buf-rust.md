# Draft: routing the Rust demo extproc through buf

> **Status: draft.** Living document. Proposed, not implemented. Not yet reviewed.
> Companion to the existing buf setup used for the Go side of the demo.

## Background

The demo keeps a single source of truth for its API definitions: the `proto/`
module (`controller.v1`). Two code paths exist today:

- **Go:** `buf generate` (via `buf.gen.yaml`, managed mode, Go plugins →
  `gen/go`) is the codegen entry point. `just gen` runs `buf generate`.
- **Rust (draft):** `rust/demo-extproc/build.rs` calls
  `tonic_prost_build::configure().compile_protos(&["../../proto/controller/v1/controller.proto"],
  &["../../proto"])`, which runs protoc through prost-build. Generated code lands in the
  Cargo `OUT_DIR`; `rust/demo-extproc/src/lib.rs` pulls it in via
  `include!(concat!(env!("OUT_DIR"), "/controller.v1.rs"))`.

The Rust path invokes protoc/tonic-prost-build directly against the `proto/` tree. Buf
therefore lints, breaks, formats and generates Go, but the Rust codegen bypasses buf's plugin
machinery entirely.

## Goal

- `buf generate` (i.e. `just gen`) produces Go **and** Rust code from the same `proto/` module.
- Buf is the single codegen orchestrator and source of truth for the demo's protobuf surface.
- The Rust build no longer invokes protoc/tonic-prost-build against `proto/` directly; it
  consumes buf-generated output.
- The Go and Rust generated code stay in sync with `proto/` automatically, via the existing
  buf workflow (`buf lint`, `buf breaking`, `buf format`, and the CI Proto Sync Check).

## Proposed design (draft)

### Chosen mechanism (this draft): buf emits a FileDescriptorSet; Rust generates from it

**Why this direction:** the Rust codegen stack (prost / tonic-prost / tonic-build) does not
ship a standalone `protoc-gen-*` plugin binary. Its generation entry points are the
prost-build/tonic-prost-build APIs. Buf drives codegen by invoking protoc plugins, so the
direct way to get Rust codegen "through buf" without inventing a plugin binary is to have buf
emit the **FileDescriptorSet** and have the Rust build script generate Rust from that FDS via
the existing public API:

- `tonic_prost_build::configure().compile_fds(fds)` (tonic-prost-build), or
- `prost_build::Config::new().file_descriptor_set_path(..).skip_protoc_run(true).compile_protos(..)`
  then `prost_build::Config::compile_fds(fds)`.

This keeps the Rust side on the same generator (prost/tonic-prost) that the draft already uses,
so the generated module/naming surface is unchanged.

**Buf side (draft):**

Add a plugin entry to `buf.gen.yaml` that emits the descriptor set for the module, e.g. a local
`protoc-gen-descriptor` plugin writing into a Rust output directory:

```yaml
version: v2
managed:
  enabled: true
  override:
    - file_option: go_package_prefix
      value: github.com/wrossmorrow/envoy-extproc-sdk-site/gen/go
plugins:
  - local: ["go", "tool", "protoc-gen-go"]
    out: gen/go
    opt: paths=source_relative
  - local: ["go", "tool", "protoc-gen-go-grpc"]
    out: gen/go
    opt: paths=source_relative
  - local: ["go", "tool", "protoc-gen-connect-go"]
    out: gen/go
    opt: paths=source_relative
  # DRAFT: emit a FileDescriptorSet for the Rust side. The Rust stack has no
  # standalone protoc-gen plugin, so buf emits the FDS and the Rust build script
  # generates from it via the tonic-prost-build FDS API.
  - local: ["protoc-gen-descriptor"]
    out: gen/rust
    opt: "--include_imports"
```

The `--include_imports` flag matters: prost-build always compiles with
`--include_imports`, and the demo's protos import
`google/protobuf/timestamp.proto`, so the FDS handed to the Rust generator must carry imports.

**Rust side (draft):**

`rust/demo-extproc/build.rs` changes from compiling protos to generating from an FDS. Using
`tonic_prost_build::configure()` (rather than the bare helpers) keeps the same defaults the draft
already relies on (`build_client`, `build_server`, `build_transport`):

```rust
fn main() {
    // FileDescriptorSet emitted by `buf generate` (see buf.gen.yaml), read from its
    // checked-in location. Decoding requires the relevant prost crates in
    // `[build-dependencies]` of the demo crate.
    let fds = prost_types::FileDescriptorSet::decode(
        &std::fs::read("../../gen/rust/controller/v1/descriptor.pb").unwrap()[..],
    ).unwrap();

    tonic_prost_build::configure()
        .compile_fds(fds)
        .unwrap();
}
```

The existing include in `rust/demo-extproc/src/lib.rs`
(`include!(concat!(env!("OUT_DIR"), "/controller.v1.rs"))`) keeps working unchanged for
FDS-based generation, because generated file/module naming is derived from the proto package:
verified locally that FDS-based generation produces the same `controller.v1.rs` module, the same
module nesting (`controller::v1`), the same clients/servers
(`DemoControllerServiceClient` / `DemoControllerServiceServer`,
`DemoProcessorServiceClient` / `DemoProcessorServiceServer`), and the same message modules
(such as `decision::Action`). The only observed delta was the absence of the
`#[allow(clippy::all)]` attribute that the proto-compiling path adds; carry it via
`prost_build::Config` if clippy cleanliness is required.

Locally validated (scratch change, since reverted): FDS produced via
`prost_build::Config::file_descriptor_set_path` + `compile_protos`, then
`tonic_prost_build::configure().compile_fds(fds)` compiled and passed the crate's unit tests
(8 passed). The two mechanisms generate equivalent Rust module surfaces, so the draft's Rust-side
change is behavior-preserving for the current protos. The remaining step for the real flow is
replacing the locally-produced FDS with the one emitted by buf via the descriptor plugin entry
in `buf.gen.yaml`.

### Alternative mechanism (draft): buf-invoked local Rust protoc plugin

An alternative is to add a local Rust protoc plugin (e.g. `protoc-gen-tonic-prost`) that wraps
the tonic-prost service generator and have buf run it via a `local:` plugin entry, with generated
Rust written to a checked-in source directory (mirroring the `gen/go` pattern) that the crate
includes via `include!`.

This is the more "buf-native" end state, but it requires implementing/shipping a standalone
plugin for the prost/tonic-prost codegen path and has not yet been done upstream. It is the
preferred fallback if the FDS-emission route proves awkward for the team.

## Concrete changes proposed (not yet applied)

1. `buf.gen.yaml`: add the Rust FDS-emit plugin entry (drafted above).
2. Rust build input: read the FDS emitted by buf and switch the generator to the FDS-based API.
3. `justfile`: keep `gen: buf generate` as the single entry point; no change required if buf
   emits both Go and Rust artifacts. Consider a separate `gen:rust` alias only if artifact layout
   diverges.
4. CI (`.github/workflows/codegen.yaml`): extend the Proto Sync Check so the Rust artifact
   (FDS file and/or generated output) is regenerated and verified alongside `gen/go`.

## Verification plan (draft)

- `just gen` produces Go and Rust artifacts from `proto/`.
- `cargo build` and `cargo test` in `rust/demo-extproc` still pass with the FDS-based generation.
- `buf lint`, `buf breaking`, `buf format` clean on `proto/`.
- CI Proto Sync Check passes for both Go and Rust artifacts.

## Open questions

- **FDS emission mechanism:** whether/how buf can run the descriptor plugin reliably for this
  module, and what output path/naming the Rust side should consume. Fallback: adopt the local
  Rust plugin route. Locally validated that an FDS built from the same protos flows through the
  public `tonic_prost_build` FDS API cleanly; the buf-side emission remains to be validated.
- **Generated Rust artifact layout:** whether Rust generated output should be checked in
  (e.g. under a new `gen/rust/` or `rust/demo-extproc/src/generated/` directory, mirroring
  `gen/go`) or kept in Cargo `OUT_DIR`. The draft leans toward checked-in FDS output for parity
  with the Go workflow, pending maintainer preference. With the FDS approach the FDS can live in
  a checked-in directory and be read by the build script, while Cargo `OUT_DIR` remains the
  generated-Rust destination.
- **CI:** whether and how the sync check should cover Rust artifacts; not changed in this draft.
- **Behavior parity:** FDS-based generation must reproduce the exact generated module/naming
  surface the draft's processor code depends on (file names, module nesting, client/server
  trait shapes). Verified locally for the current protos; must be re-verified after any proto
  changes.
