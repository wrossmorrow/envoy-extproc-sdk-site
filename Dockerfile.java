ARG JAVA_VERSION=21
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.39
FROM eclipse-temurin:${JAVA_VERSION}-jdk AS builder

ARG GITHUB_ACTOR
WORKDIR /home/demo
COPY java/demo-extproc/build.gradle build.gradle
COPY java/demo-extproc/settings.gradle settings.gradle
COPY java/demo-extproc/gradle/ gradle/
COPY --chmod=0755 java/demo-extproc/gradlew gradlew
COPY java/demo-extproc/src/ src/
COPY gen/ gen/
RUN --mount=type=secret,id=github_token \
    ./gradlew --no-daemon installDist \
    -PgeneratedCodeDir=/home/demo/gen/java \
    -Pgpr.user="${GITHUB_ACTOR}" \
    -Pgpr.key="$(cat /run/secrets/github_token)"

FROM eclipse-temurin:${JAVA_VERSION}-jre
COPY --from=builder /home/demo/build/install/demo-extproc/ /opt/demo-extproc
ARG GRPC_HEALTH_PROBE_VERSION
ARG TARGETARCH
RUN curl -fsSL "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}" \
    -o /grpc-health-probe && \
    chmod +x /grpc-health-probe
ENTRYPOINT ["/opt/demo-extproc/bin/demo-extproc"]
