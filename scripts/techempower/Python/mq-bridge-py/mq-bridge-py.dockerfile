# TechEmpower Docker image for the mq-bridge-py (Python) entry.
#
# Builds the mq-bridge wheel with maturin (http feature only) and runs server.py.
# Build and runtime use the same Python base image so the compiled extension ABI
# matches. For the upstream PR, pin MQB_REF to a released tag.
FROM python:3.12-slim AS build

ARG MQB_REF=python
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
RUN pip install --no-cache-dir maturin

RUN git clone --depth 1 -b "${MQB_REF}" https://github.com/marcomq/mq-bridge /src
WORKDIR /src/python/mq-bridge-py
RUN maturin build --release --no-default-features -F http -F pyo3/extension-module -o /wheels

FROM python:3.12-slim
COPY --from=build /wheels/*.whl /wheels/
RUN pip install --no-cache-dir /wheels/*.whl
RUN groupadd --system mqbridge \
    && useradd --system --gid mqbridge --home-dir /app --shell /usr/sbin/nologin mqbridge \
    && mkdir -p /app \
    && chown -R mqbridge:mqbridge /app
COPY --chown=mqbridge:mqbridge server.py /app/server.py
WORKDIR /app
EXPOSE 8080
USER mqbridge:mqbridge
CMD ["python", "server.py"]
