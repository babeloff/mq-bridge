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
  http      - generate CA and cert/key for HTTP TLS tests (output: $OUT_ROOT/http-certs)
  grpc      - generate CA and cert/key for gRPC TLS tests (output: $OUT_ROOT/grpc-certs)
EOF
  exit 1
}

if [[ -z "$CMD" ]]; then
  usage
fi

BASE_CERT_DIR="$OUT_ROOT/certs"

ensure_base_certs() {
  mkdir -p "$BASE_CERT_DIR"
  # If server.pem already exists assume base certs present
  if [[ -f "$BASE_CERT_DIR/server.pem" && -f "$BASE_CERT_DIR/ca.pem" ]]; then
    echo "Base certs already present in $BASE_CERT_DIR"
    return
  fi

  echo "Generating base server certs in $BASE_CERT_DIR"
  openssl genrsa -out "$BASE_CERT_DIR/ca.key" 4096
  openssl req -x509 -new -nodes -key "$BASE_CERT_DIR/ca.key" -sha256 -days 3650 -subj "/CN=MQBridge Test CA" -out "$BASE_CERT_DIR/ca.pem"

  openssl genrsa -out "$BASE_CERT_DIR/server.key" 4096
  openssl req -new -key "$BASE_CERT_DIR/server.key" -subj "/CN=localhost" -out "$BASE_CERT_DIR/server.csr"

  cat > "$BASE_CERT_DIR/openssl.cnf" <<'EOF'
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

  openssl x509 -req -in "$BASE_CERT_DIR/server.csr" -CA "$BASE_CERT_DIR/ca.pem" -CAkey "$BASE_CERT_DIR/ca.key" -CAcreateserial -out "$BASE_CERT_DIR/server.crt" -days 3650 -sha256 -extfile "$BASE_CERT_DIR/openssl.cnf" -extensions v3_req
  cat "$BASE_CERT_DIR/server.key" "$BASE_CERT_DIR/server.crt" > "$BASE_CERT_DIR/server.pem"

  # Do NOT create legacy mongo.* names; use server.* exclusively

  chmod 644 "$BASE_CERT_DIR"/* || true
  # ls -l "$BASE_CERT_DIR"
}

generate_pem_dir() {
  local outdir="$1"
  mkdir -p "$outdir"
  echo "Ensuring base certs and copying server.* to $outdir"
  ensure_base_certs
  cp -a "$BASE_CERT_DIR/ca.pem" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.crt" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.key" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.pem" "$outdir/" 2>/dev/null || true
  chmod 644 "$outdir"/* || true
  # ls -l "$outdir"
}

generate_ibm_mq_dir() {
  local outdir="$1"
  local label_dir="$outdir/keys/ibmmqkey"
  local trust_dir="$outdir/trust/0"
  mkdir -p "$outdir" "$label_dir" "$trust_dir"
  echo "Ensuring base certs and copying IBM MQ TLS materials to $outdir"
  ensure_base_certs
  cp -a "$BASE_CERT_DIR/ca.pem" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.crt" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.key" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.pem" "$outdir/" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.key" "$label_dir/tls.key" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/server.crt" "$label_dir/tls.crt" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/ca.pem" "$label_dir/ca.crt" 2>/dev/null || true
  cp -a "$BASE_CERT_DIR/ca.pem" "$trust_dir/tls.crt" 2>/dev/null || true
  chmod 644 "$outdir"/* || true
  chmod 644 "$label_dir"/* || true
  chmod 644 "$trust_dir"/* || true
}

generate_keystore_dir() {
  local outdir="$1"
  mkdir -p "$outdir"
  echo "Generating keystore artifacts in $outdir"
  ensure_base_certs
  STOREPASS=changeit
  KEYPASS=changeit
  if [[ -f "$BASE_CERT_DIR/ca.pem" ]]; then
    cp -a "$BASE_CERT_DIR/ca.pem" "$outdir/" 2>/dev/null || true
    cp -a "$BASE_CERT_DIR/ca.key" "$outdir/" 2>/dev/null || true
  else
    openssl genrsa -out "$outdir/ca.key" 4096
    openssl req -x509 -new -nodes -key "$outdir/ca.key" -sha256 -days 3650 -subj "/CN=MQBridge Test CA" -out "$outdir/ca.pem"
  fi
  openssl genrsa -out "$outdir/kafka.key" 2048
  cat > "$outdir/openssl.cnf" <<'EOF'
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
  openssl req -new -key "$outdir/kafka.key" -subj "/CN=localhost" -out "$outdir/kafka.csr" -config "$outdir/openssl.cnf"
  openssl x509 -req -in "$outdir/kafka.csr" -CA "$outdir/ca.pem" -CAkey "$outdir/ca.key" -CAcreateserial -out "$outdir/kafka.crt" -days 3650 -sha256 -extfile "$outdir/openssl.cnf" -extensions v3_req
  cat "$outdir/kafka.key" "$outdir/kafka.crt" > "$outdir/kafka.pem"
  openssl pkcs12 -export -in "$outdir/kafka.crt" -inkey "$outdir/kafka.key" -certfile "$outdir/ca.pem" -name kafka -passout pass:$STOREPASS -out "$outdir/kafka.pkcs12"
  if command -v keytool >/dev/null 2>&1; then
    keytool -importkeystore -deststorepass $STOREPASS -destkeypass $KEYPASS -destkeystore "$outdir/kafka.keystore.jks" -srckeystore "$outdir/kafka.pkcs12" -srcstoretype PKCS12 -srcstorepass $STOREPASS -alias kafka
    keytool -import -trustcacerts -keystore "$outdir/kafka.truststore.jks" -storepass $STOREPASS -noprompt -alias CARoot -file "$outdir/ca.pem"
  else
    echo "keytool not found; pkcs12 created but JKS keystore/truststore not created"
  fi
  chmod 644 "$outdir"/* || true
  # ls -l "$outdir"
}

case "$CMD" in
  pem)
    generate_pem_dir "$OUT_ROOT/pem"
    ;;
  keystore)
    generate_keystore_dir "$OUT_ROOT/keystore"
    ;;
  mongodb|http|grpc|ibm-mq)
    case "$CMD" in
      mongodb) OUT_DIR="$OUT_ROOT/certs" ;;
      http) OUT_DIR="$OUT_ROOT/http-certs" ;;
      grpc) OUT_DIR="$OUT_ROOT/grpc-certs" ;;
      ibm-mq) OUT_DIR="$OUT_ROOT/ibm-mq-certs" ;;
    esac
    if [[ "$CMD" == "ibm-mq" ]]; then
      generate_ibm_mq_dir "$OUT_DIR"
    else
      generate_pem_dir "$OUT_DIR"
    fi
    ;;
  kafka)
    generate_keystore_dir "$OUT_ROOT/kafka-certs"
    ;;
  *)
    usage
    ;;
esac
