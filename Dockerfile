FROM rust:bookworm as build
ARG my_appname
ENV my_appname=$my_appname

WORKDIR /workspace/

COPY ./ ./
RUN touch ./src/main.rs

# build, with dependency cache
RUN --mount=type=cache,target=/usr/local/cargo/registry,id="reg-${TARGETPLATFORM}" \
    --mount=type=cache,target=target,id="target-demon-${TARGETPLATFORM}" \
    cargo build --release && \
    mkdir bin && \
    mv target/release/demon bin/demon


# our final base
FROM debian:bookworm-slim

# install OS dependencies
RUN apt-get update && apt-get install -y ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

# copy the build artifact from the build cache
COPY --from=build /workspace/bin/demon /usr/local/bin/demon

# set the startup command to run your binary
CMD demon
