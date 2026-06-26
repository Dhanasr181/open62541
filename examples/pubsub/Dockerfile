# ════════════════════════════════════════════════════════════════════════════
#  Stage 1 – Build open62541 + our OPC UA Azure apps
# ════════════════════════════════════════════════════════════════════════════
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        libssl-dev \
        python3 \
        python3-pip \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ── Clone open62541 (pin to stable 1.4 tag) ─────────────────────────────
ARG OPEN62541_VERSION=v1.4.6
WORKDIR /opt
RUN git clone --depth 1 --branch ${OPEN62541_VERSION} \
        https://github.com/open62541/open62541.git \
    && cd open62541 \
    && git submodule update --init --recursive

# ── Build open62541 with MQTT + JSON + TLS (OpenSSL) ────────────────────
WORKDIR /opt/open62541/build
RUN cmake .. \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DUA_ENABLE_PUBSUB=ON \
        -DUA_ENABLE_PUBSUB_DELTAFRAMES=ON \
        -DUA_ENABLE_PUBSUB_MONITORING=ON \
        -DUA_ENABLE_MQTT=ON \
        -DUA_ENABLE_JSON_ENCODING=ON \
        -DUA_ENABLE_ENCRYPTION=OPENSSL \
        -DUA_BUILD_EXAMPLES=OFF \
    && make -j"$(nproc)" \
    && make install

# ── Build our applications ────────────────────────────────────────────────
WORKDIR /opt/opcua-azure/build
COPY CMakeLists.txt /opt/opcua-azure/
COPY src/            /opt/opcua-azure/src/

RUN cmake /opt/opcua-azure \
        -DCMAKE_BUILD_TYPE=RelWithDebInfo \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && make -j"$(nproc)" \
    && make install

# ════════════════════════════════════════════════════════════════════════════
#  Stage 2 – Runtime image (much smaller)
# ════════════════════════════════════════════════════════════════════════════
FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy built binaries and shared libs from builder
COPY --from=builder /usr/local/bin/azure_pubsub_publisher   /usr/local/bin/
COPY --from=builder /usr/local/bin/azure_pubsub_subscriber  /usr/local/bin/
COPY --from=builder /usr/local/lib/libopen62541*             /usr/local/lib/

RUN ldconfig

# Certs are mounted at runtime – create the mount-point directory
RUN mkdir -p /certs

# ── Publisher is the default CMD; override in docker-compose for subscriber
CMD ["/usr/local/bin/azure_pubsub_publisher"]
