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
    docker run --rm -v "${PWD}":/work -w /work --entrypoint buf \
        ghcr.io/wrossmorrow/flint-pi-golang generate

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

# bring up compose
up *flags="": gen
    docker compose up --build {{flags}}

# bring down compose
down *flags="":
    docker compose down {{flags}}
