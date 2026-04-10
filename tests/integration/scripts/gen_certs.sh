#!/usr/bin/env bash
set -euo pipefail

CMD=${1:-}
OUT_ROOT="$(cd "$(dirname "$0")/.." && pwd)/docker-compose"

usage() {
  cat <<EOF
Usage: $0 <service>
Services:
  mongodb   - generate CA and PEM for MongoDB (output: $OUT_ROOT/certs)
  kafka     - generate CA and keystore/truststore for Kafka (output: $OUT_ROOT/kafka-certs)
EOF
  exit 1
}

if [[ -z "$CMD" ]]; then
  usage
fi

case "$CMD" in
  mongodb)
    OUT_DIR="$OUT_ROOT/certs"
    mkdir -p "$OUT_DIR"
    echo "Generating MongoDB certs in $OUT_DIR"
    openssl genrsa -out "$OUT_DIR/ca.key" 4096
    openssl req -x509 -new -nodes -key "$OUT_DIR/ca.key" -sha256 -days 3650 -subj "/CN=MQBridge Test CA" -out "$OUT_DIR/ca.pem"
    openssl genrsa -out "$OUT_DIR/mongo.key" 4096
    openssl req -new -key "$OUT_DIR/mongo.key" -subj "/CN=localhost" -out "$OUT_DIR/mongo.csr"
    cat > "$OUT_DIR/openssl.cnf" <<'EOF'
[ req ]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[ req_distinguished_name ]

[ v3_req ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl x509 -req -in "$OUT_DIR/mongo.csr" -CA "$OUT_DIR/ca.pem" -CAkey "$OUT_DIR/ca.key" -CAcreateserial -out "$OUT_DIR/mongo.crt" -days 3650 -sha256 -extfile "$OUT_DIR/openssl.cnf" -extensions v3_req
    cat "$OUT_DIR/mongo.key" "$OUT_DIR/mongo.crt" > "$OUT_DIR/mongo.pem"
    chmod 644 "$OUT_DIR"/*
    ls -l "$OUT_DIR"
    ;;
  kafka)
    OUT_DIR="$OUT_ROOT/kafka-certs"
    mkdir -p "$OUT_DIR"
    echo "Generating Kafka certs in $OUT_DIR"
    STOREPASS=changeit
    KEYPASS=changeit
    openssl genrsa -out "$OUT_DIR/ca.key" 4096
    openssl req -x509 -new -nodes -key "$OUT_DIR/ca.key" -sha256 -days 3650 -subj "/CN=MQBridge Test CA" -out "$OUT_DIR/ca.pem"
    openssl genrsa -out "$OUT_DIR/kafka.key" 2048
    cat > "$OUT_DIR/openssl.cnf" <<'EOF'
[ req ]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[ req_distinguished_name ]

[ v3_req ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl req -new -key "$OUT_DIR/kafka.key" -subj "/CN=localhost" -out "$OUT_DIR/kafka.csr" -config "$OUT_DIR/openssl.cnf"
    openssl x509 -req -in "$OUT_DIR/kafka.csr" -CA "$OUT_DIR/ca.pem" -CAkey "$OUT_DIR/ca.key" -CAcreateserial -out "$OUT_DIR/kafka.crt" -days 3650 -sha256 -extfile "$OUT_DIR/openssl.cnf" -extensions v3_req
    cat "$OUT_DIR/kafka.key" "$OUT_DIR/kafka.crt" > "$OUT_DIR/kafka.pem"
    openssl pkcs12 -export -in "$OUT_DIR/kafka.crt" -inkey "$OUT_DIR/kafka.key" -certfile "$OUT_DIR/ca.pem" -name kafka -passout pass:$STOREPASS -out "$OUT_DIR/kafka.pkcs12"
    if command -v keytool >/dev/null 2>&1; then
      keytool -importkeystore -deststorepass $STOREPASS -destkeypass $KEYPASS -destkeystore "$OUT_DIR/kafka.keystore.jks" -srckeystore "$OUT_DIR/kafka.pkcs12" -srcstoretype PKCS12 -srcstorepass $STOREPASS -alias kafka
      keytool -import -trustcacerts -keystore "$OUT_DIR/kafka.truststore.jks" -storepass $STOREPASS -noprompt -alias CARoot -file "$OUT_DIR/ca.pem"
    else
      echo "keytool not found; pkcs12 created but JKS keystore/truststore not created"
    fi
    chmod 644 "$OUT_DIR"/*
    ls -l "$OUT_DIR"
    ;;
  ibm-mq)
    OUT_DIR="$OUT_ROOT/ibm-mq-certs"
    mkdir -p "$OUT_DIR"
    echo "Generating IBM MQ certs in $OUT_DIR"
    openssl genrsa -out "$OUT_DIR/ca.key" 4096
    openssl req -x509 -new -nodes -key "$OUT_DIR/ca.key" -sha256 -days 3650 -subj "/CN=MQBridge Test CA" -out "$OUT_DIR/ca.pem"
    openssl genrsa -out "$OUT_DIR/mq.key" 2048
    cat > "$OUT_DIR/openssl.cnf" <<'EOF'
[ req ]
distinguished_name = req_distinguished_name
req_extensions = v3_req

[ req_distinguished_name ]

[ v3_req ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF
    openssl req -new -key "$OUT_DIR/mq.key" -subj "/CN=localhost" -out "$OUT_DIR/mq.csr" -config "$OUT_DIR/openssl.cnf"
    openssl x509 -req -in "$OUT_DIR/mq.csr" -CA "$OUT_DIR/ca.pem" -CAkey "$OUT_DIR/ca.key" -CAcreateserial -out "$OUT_DIR/mq.crt" -days 3650 -sha256 -extfile "$OUT_DIR/openssl.cnf" -extensions v3_req
    cat "$OUT_DIR/mq.key" "$OUT_DIR/mq.crt" > "$OUT_DIR/mq.pem"
    chmod 644 "$OUT_DIR"/*
    ls -l "$OUT_DIR"
    ;;
  *)
    usage
    ;;
esac
