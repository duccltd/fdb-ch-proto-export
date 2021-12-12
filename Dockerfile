FROM rust:1.54.0-buster as builder

RUN rustup component add rustfmt 

RUN apt-get update && apt-get install libclang-dev -y

RUN curl -O https://www.foundationdb.org/downloads/6.2.25/ubuntu/installers/foundationdb-clients_6.2.25-1_amd64.deb && \
    dpkg -i foundationdb-clients_6.2.25-1_amd64.deb

WORKDIR /app

COPY . .

RUN rustup default nightly && rustup update

RUN cargo build --release

# ----------------

FROM debian:buster-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    tini \
    curl;

RUN curl -O https://www.foundationdb.org/downloads/6.2.25/ubuntu/installers/foundationdb-clients_6.2.25-1_amd64.deb && \
    dpkg -i foundationdb-clients_6.2.25-1_amd64.deb

COPY --from=builder /app/target/release/fdb-ch-proto-export .

ENTRYPOINT [ "/fdb-ch-proto-export" ]