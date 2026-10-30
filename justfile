
# list targets and exit
default:
    just --list

# geenrate code
gen:
    docker run --rm -v "${PWD}":/work -w /work --entrypoint buf \
        ghcr.io/wrossmorrow/flint-pi-golang generate

