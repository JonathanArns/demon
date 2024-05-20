FROM python:3.11-slim-bookworm 

# setup ssh server
RUN apt-get update && apt-get install -y openssh-server
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN useradd -m -s /bin/bash bilbo
RUN echo "bilbo:insecure_password" | chpasswd
EXPOSE 22

RUN pip install execnet requests

WORKDIR /workspace/
COPY ./py-tpcc /workspace/py-tpcc

ENTRYPOINT /bin/sh -c "service ssh start && tail -f /dev/null"
