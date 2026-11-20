# Plan: Java demo extproc (envoy-extproc-sdk-java 0.3.0-alpha1)

> **Status: implemented.** Living document. Will change. Not yet reviewed.

## Goal

Mirror the interactive demo extproc (`internal/extproc` in Go,
`rust/demo-extproc` in Rust) in Java 21, built on the
[`envoy-extproc-sdk-java`](https://github.com/wrossmorrow/envoy-extproc-sdk-java)
release **0.3.0-alpha1** and speaking the same `controller/v1` proto over the same
`DemoProcessorService.Process` bidi channel.

The controller (`internal/controller`) is deliberately **not** re-implemented in
Java: the Java demo extproc is a client of the existing controller, exactly like
the Go and Rust demo extprocs.

## Steps

- [x] **Step 1 — Java build wiring.** Add a Gradle build for the Java demo
      extproc (JDK 21), consuming the `envoy-extproc-sdk-java` **0.3.0-alpha1**
      artifact (built locally from the tag and published to the local Maven
      repo) and the buf-generated `controller/v1` Java gRPC client code.
- [x] **Step 2 — buf codegen path for Java.** Extend `buf.gen.yaml` with the
      `protoc_builtin: java` plugin and a local `protoc-gen-grpc-java` plugin so
      `buf generate` produces `gen/java` alongside `gen/go` and `gen/rust`; document
      the path in `docs/buf-java.md`.
- [x] **Step 3 — DemoRequestProcessor.** Implement the `RequestProcessor`
      demo processor: phase handlers, controller bidi client stream per exchange
      (held in the request context custom data), phase snapshot, decision
      application (continue/cancel), fail-closed error handler, exchange timeout,
      env-var configuration (`CONTROLLER_ADDR`, `EXCHANGE_TIMEOUT`,
      `ABORT_ON_PROCESSOR_FAILURE`).
- [x] **Step 4 — Demo server entrypoint.** Wire the demo processor into
      `ExternalProcessorServer` (`serve`/`mustServe`) and add the `cmd`-style
      main class.
- [x] **Step 5 — Tests.** Replicate the Go demo extproc tests in Java and add
      coverage for the pieces the Go tests do not reach:
      - env-var wiring (controller addr, exchange timeout, abort flag);
      - phase snapshot (status, body truncation, masking, sorting, `:method`/`:path`);
      - session id resolution (context custom value vs. headers);
      - decision application (continue mutations / cancel);
      - controller bidi flow end to end against an in-process fake
        `DemoProcessorService` server (continue, cancel, phase timeout fail-closed);
      - full two-phase continue flow across phases.

## Open questions

- Whether the Gradle build lives at the repo root or under `java/demo-extproc/`.
- Whether to consume the SDK via a local `publishToMavenLocal` artifact or a
  Gradle composite build from a local SDK checkout.
- Whether the extproc-side exchange timeout should itself emit an immediate
  cancel decision (Go today propagates the error to the fail-closed abort path).
- Frontend wiring for the new `java` demo language (separate from this task).

## Out of scope (for this PR)

- Controller implementation in Java (the Go controller is the controller).
- Proto changes (the `controller/v1` surface is settled).
- Frontend (`web`) changes.
- CI workflow changes (see repo instructions: never modify CI config here).
