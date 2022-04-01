FROM rust:1.54.0-buster as builder

RUN rustup component add rustfmt 

RUN apt-get update && apt-get install libclang-dev -y

RUN curl -O -L https://github.com/ducc/musical-octo-doodle/releases/download/6.2.25/foundationdb-clients_6.2.25-0.c08b1a84f471e9adab5229cc2bb25afb60e1e0ab.PRERELEASE_amd64.deb && \
    dpkg -i foundationdb-clients_6.2.25-0.c08b1a84f471e9adab5229cc2bb25afb60e1e0ab.PRERELEASE_amd64.deb

WORKDIR /app

COPY . .

RUN rustup default nightly && rustup update

RUN cargo build --release

FROM debian:buster-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    tini \
    curl;

RUN curl -O -L https://github.com/ducc/musical-octo-doodle/releases/download/6.2.25/foundationdb-clients_6.2.25-0.c08b1a84f471e9adab5229cc2bb25afb60e1e0ab.PRERELEASE_amd64.deb && \
    dpkg -i foundationdb-clients_6.2.25-0.c08b1a84f471e9adab5229cc2bb25afb60e1e0ab.PRERELEASE_amd64.deb

COPY --from=builder /app/target/release/fdb-ch-proto-export .
COPY --from=builder /app/google_protobuf /google_protobuf

ENTRYPOINT [ "/fdb-ch-proto-export" ]
