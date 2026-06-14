# TechEmpower Docker image for the mq-bridge (Rust) entry.
#
# The crate uses a path dependency on the mq-bridge repo, so we clone the repo
# and build this example from inside it. For the upstream PR, the clone is
# pinned to released tag v0.2.17; update when a newer release tag exists.
FROM rust:1-bookworm AS build

ARG MQB_REF=v0.2.17
RUN git clone --depth 1 -b "${MQB_REF}" https://github.com/marcomq/mq-bridge /src
WORKDIR /src/scripts/techempower/Rust/mq-bridge
RUN cargo build --release

FROM debian:bookworm-slim
RUN groupadd --system mqbridge \
    && useradd --system --gid mqbridge --home-dir /nonexistent --shell /usr/sbin/nologin mqbridge
COPY --from=build --chown=mqbridge:mqbridge /src/scripts/techempower/Rust/mq-bridge/target/release/mq-bridge-techempower /usr/local/bin/mq-bridge-techempower
# TechEmpower runs the database as host `tfb-database`. Connection failure is
# non-fatal, so the JSON/Plaintext tests still run if the DB is absent.
ENV DATABASE_URL="postgres://benchmarkdbuser:benchmarkdbpass@tfb-database:5432/hello_world"
EXPOSE 8080
USER mqbridge:mqbridge
CMD ["mq-bridge-techempower"]
