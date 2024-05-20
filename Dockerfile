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
FROM python:3.11-slim-bookworm 

RUN apt-get update && apt-get install -y openssh-server ca-certificates libssl-dev iproute2 && rm -rf /var/lib/apt/lists/*
RUN pip install flask execnet requests

# setup ssh server for distributed tpcc
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN useradd -m -s /bin/bash bilbo
RUN echo "bilbo:insecure_password" | chpasswd
EXPOSE 22
COPY ./py-tpcc /workspace/py-tpcc

COPY ./server_controller.py ./server_controller.py
COPY --from=build /workspace/bin/demon /usr/local/bin/demon

ENTRYPOINT /bin/sh -c "service ssh start && python server_controller.py" 
