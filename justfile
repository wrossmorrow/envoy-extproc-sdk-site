image_name := "ghcr.io/wrossmorrow/envoy-extproc-sdk-site"
image_tag := `git rev-parse --short HEAD`

# list targets and exit
default:
    just --list

# update dependendencies
update:
    go get -u && just tidy

# cleanup modules
tidy:
    go mod tidy

# generate code for protobufs
gen:
    buf generate

# build the rust demo extproc
rust-build:
    cd rust/demo-extproc && cargo build

# run the rust demo extproc unit tests
rust-test:
    cd rust/demo-extproc && cargo test

# format the rust code
rust-fmt:
    cd rust/demo-extproc && cargo fmt
    cd rust/protoc-gen-tonic-prost && cargo fmt

# lint the rust code
rust-lint:
    cd rust/demo-extproc && cargo clippy --all-targets -- -D warnings
    cd rust/protoc-gen-tonic-prost && cargo clippy --all-targets -- -D warnings

# format code
format:
    go fmt ./...

# run linter
lint:
    golangci-lint run ./...

# run vetter
vet: gen
    go vet ./...

# run unit tests
unit-test: gen
    go test -v ./...

# run unit tests (standard)
test: gen
    go test ./...

# run a specific cmd
run cmd *flags="":
    go run ./cmd/{{cmd}} {{flags}}

# build binary (variadic flags supported)
build cmd *flags="": gen
    go build -o bin/{{cmd}} {{ flags }} ./cmd/{{cmd}}

# containerize one of the commands
containerize cmd tag=image_tag *flags="": gen
    docker build --build-arg CMD={{cmd}} -t {{ image_name }}-{{cmd}}:{{ tag }} . {{ flags }}

# build the java demo extproc
java-build:
    cd java/demo-extproc && ./gradlew --no-daemon build

# run the java demo extproc unit tests
java-test:
    cd java/demo-extproc && ./gradlew --no-daemon test

# run the java demo extproc
java-run:
    cd java/demo-extproc && ./gradlew --no-daemon run

# publish the envoy extproc sdk locally (from the envoy-extproc-sdk-java checkout)
java-sdk-install:
    cd /tmp/envoy-extproc-sdk-java && ./gradlew publishToMavenLocal -PreleaseVersion=0.3.0-alpha1

# containerize the rust demo extproc
containerize-rust tag=image_tag:
    docker build -f Dockerfile.rust -t {{ image_name }}-extproc:{{ tag }} .

# containerize the java demo extproc
containerize-java tag=image_tag:
    docker build -f Dockerfile.java -t {{ image_name }}-extproc-java:{{ tag }} .

# bring up compose
up *flags="": gen
    docker compose up --build {{flags}}

# bring down compose
down *flags="":
    docker compose down {{flags}}
