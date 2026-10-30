# envoy-extproc-sdk-site

## Layout
- `proto/` is the only place .proto files live; paths mirror packages
  (`proto/demo/controller/v1/` ↔ `package demo.controller.v1`). `buf lint` (STANDARD) must pass.
- `gen/go/` holds generated code only. Regenerate with `just gen`; never edit it by hand.
- `demo/`: envoy external processor, implements a client for `DemoProcessor`
- `controller/`: connectrpc server for `DemoController`, gRPC server for `DemoProcessor`

## Contracts
- Services talk only through the protos in `proto/`; no shared Go packages across services
  except `gen/go`.
- `rpc DemoProcessor.Process` (bidi streaming): `demo/` implementation opens bidi stream, `controller/` handles and responds. 
- `rpc DemoController.CreateSession` (server-side streaming): browser/user opens, `controller/` implementation handles
- `rpc DemoController.SubmitDecision` (unary): browser/user sends, `controller/` implementation handles

## Changes
- New RPCs, messages or services: propose the .proto change first and wait for approval
  before implementing.
- One concern per PR: don't mix proto changes, service logic and infra/compose changes.
