FROM rust:bookworm as build

RUN apt-get update && apt-get install -y clang

WORKDIR /workspace/

COPY ./ ./
RUN touch ./demon/src/main.rs

# build, with dependency cache
RUN --mount=type=cache,target=/usr/local/cargo/registry,id="reg-${TARGETPLATFORM}" \
    --mount=type=cache,target=target,id="target-demon-${TARGETPLATFORM}" \
    cargo build --release --bin demon && \
    mkdir bin && \
    mv target/release/demon bin/demon


# our final base
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates libssl-dev iproute2 && rm -rf /var/lib/apt/lists/*

COPY ./docker-entrypoint.sh /usr/local/bin/docker-entrypoint
COPY --from=build /workspace/bin/demon /usr/local/bin/demon

ENTRYPOINT ["docker-entrypoint"]
