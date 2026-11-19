ARG JAVA_VERSION=21
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.39
FROM eclipse-temurin:${JAVA_VERSION}-jdk AS builder

WORKDIR /home/demo-extproc-build/java/demo-extproc
COPY java/demo-extproc/build.gradle java/demo-extproc/build.gradle
COPY java/demo-extproc/settings.gradle java/demo-extproc/settings.gradle
COPY java/demo-extproc/gradle/ java/demo-extproc/gradle/
COPY java/demo-extproc/gradlew java/demo-extproc/gradlew
COPY java/demo-extproc/src/ java/demo-extproc/src/
COPY gen/java/ gen/java/
RUN ./gradlew --no-daemon installDist

FROM eclipse-temurin:${JAVA_VERSION}-jre
COPY --from=builder /home/demo-extproc-build/java/demo-extproc/build/install/demo-extproc/ /opt/demo-extproc
ARG GRPC_HEALTH_PROBE_VERSION
ARG TARGETARCH
RUN curl -fsSL "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-${TARGETARCH}" \
    -o /grpc-health-probe && \
    chmod +x /grpc-health-probe
ENTRYPOINT ["/opt/demo-extproc/bin/demo-extproc"]
