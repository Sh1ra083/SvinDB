# syntax=docker/dockerfile:1.7

FROM rust:1.97.1-bullseye@sha256:02d78ca3f928195c2a907543de778adfd728ad7e2a24fdc6aef582b7c77842e0 AS build

ARG SVINDB_BUILD_REVISION=unknown
ARG SVINDB_FEATURES=""
ARG SOURCE_DATE_EPOCH=0
ARG SOURCE_DATE_RFC3339=1970-01-01T00:00:00Z
ENV SVINDB_BUILD_REVISION=${SVINDB_BUILD_REVISION} \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} \
    CARGO_INCREMENTAL=0 \
    RUSTFLAGS="-C link-arg=-Wl,--build-id=none"

WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
RUN if [ -n "$SVINDB_FEATURES" ]; then \
        cargo build --locked --release -p svindb-server --bin svindb --features "$SVINDB_FEATURES"; \
    else \
        cargo build --locked --release -p svindb-server --bin svindb; \
    fi && \
    install -D -m 0755 target/release/svindb /runtime/usr/local/bin/svindb && \
    install -D -m 0644 /lib/x86_64-linux-gnu/libc.so.6 /runtime/lib/x86_64-linux-gnu/libc.so.6 && \
    install -D -m 0644 /lib/x86_64-linux-gnu/libm.so.6 /runtime/lib/x86_64-linux-gnu/libm.so.6 && \
    install -D -m 0644 /lib/x86_64-linux-gnu/libgcc_s.so.1 /runtime/lib/x86_64-linux-gnu/libgcc_s.so.1 && \
    install -D -m 0644 /lib/x86_64-linux-gnu/libpthread.so.0 /runtime/lib/x86_64-linux-gnu/libpthread.so.0 && \
    install -D -m 0644 /lib/x86_64-linux-gnu/libdl.so.2 /runtime/lib/x86_64-linux-gnu/libdl.so.2 && \
    install -D -m 0644 /lib/x86_64-linux-gnu/librt.so.1 /runtime/lib/x86_64-linux-gnu/librt.so.1 && \
    install -D -m 0755 /lib64/ld-linux-x86-64.so.2 /runtime/lib64/ld-linux-x86-64.so.2 && \
    install -D -m 0644 /etc/ssl/certs/ca-certificates.crt /runtime/etc/ssl/certs/ca-certificates.crt && \
    mkdir -p /runtime/data && \
    chown 65532:65532 /runtime/data

FROM scratch

ARG SVINDB_BUILD_REVISION=unknown
ARG SVINDB_BUILD_VERSION=unknown
ARG SVINDB_FEATURES=""
ARG SOURCE_DATE_EPOCH=0
ARG SOURCE_DATE_RFC3339=1970-01-01T00:00:00Z
LABEL org.opencontainers.image.title="SvinDB" \
      org.opencontainers.image.version="${SVINDB_BUILD_VERSION}" \
      org.opencontainers.image.revision="${SVINDB_BUILD_REVISION}" \
      org.opencontainers.image.features="${SVINDB_FEATURES}" \
      org.opencontainers.image.created="${SOURCE_DATE_RFC3339}" \
      org.opencontainers.image.source="https://github.com/Sh1ra083/SvinDB"

COPY --from=build /runtime/ /
USER 65532:65532
WORKDIR /data
EXPOSE 19300
ENTRYPOINT ["/usr/local/bin/svindb"]
CMD ["serve", "/data", "19300", "--bind=0.0.0.0"]
