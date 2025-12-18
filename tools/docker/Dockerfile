FROM rust:trixie

ENV AEROSPIKE_CLIENT_BRANCH=tls
ENV AEROSPIKE_HOSTS=build-trixie.:3000
ENV AEROSPIKE_USE_SERVICES_ALTERNATE=false
ENV AEROSPIKE_NAMESPACE=ns
ENV CARGO_TEST_OPTS='--no-default-features --features async,rt-tokio -- --no-capture'
ENV PROPTEST_CASES=1000

WORKDIR /app
#RUN apt update -y && \
#    apt install -y git curl build-essential

COPY startup-sequence /app/startup-sequence
#RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc
CMD "/app/startup-sequence"

