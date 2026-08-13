FROM rust:1.98.0-slim AS builder
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release -p tellus-cluster-demo \
    && mkdir -p /out/config \
    && cp target/release/node target/release/verifier /out/ \
    && cp tellus-cluster-demo/config/*.yaml /out/config/

FROM debian:trixie-slim
# iproute2 is what pumba's netem runs inside this container to cut the cluster network.
RUN apt-get update \
    && apt-get install --no-install-recommends --yes iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/node /usr/local/bin/node
COPY --from=builder /out/verifier /usr/local/bin/verifier
COPY --from=builder /out/config /opt/tellus-cluster-demo/config
WORKDIR /opt/tellus-cluster-demo
CMD ["node"]
