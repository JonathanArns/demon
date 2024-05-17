FROM python:2.7.18-slim-buster

# setup ssh server
RUN apt-get update && apt-get install -y openssh-server
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN useradd -m -s /bin/bash bilbo
RUN echo "bilbo:insecure_password" | chpasswd
EXPOSE 22

RUN pip install execnet

WORKDIR /workdir/
COPY ./py-tpcc ./

ENTRYPOINT service ssh start && bash
CMD tail -f /dev/null
