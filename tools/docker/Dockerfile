FROM debian:trixie-slim

WORKDIR /app
RUN apt update -y && \
    apt install -y git curl build-essential

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

COPY startup-sequence /app/startup-sequence
RUN chmod +x /app/startup-sequence
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc
CMD "/app/startup-sequence"

