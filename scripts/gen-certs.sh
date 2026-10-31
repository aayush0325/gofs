#!/usr/bin/env sh
# Generate a local CA, a shared server certificate for the master/chunkservers,
# and a client certificate used for outgoing gRPC connections.
set -eu

OUT="${1:-./certs}"
mkdir -p "$OUT"

if [ -f "$OUT/ca.crt" ] && [ -f "$OUT/server.crt" ] && [ -f "$OUT/client.crt" ]; then
  echo "certificates already exist in $OUT"
  exit 0
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/server.ext" <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,DNS:gfs-master,DNS:gfs-chunkserver,DNS:master,DNS:chunkserver-1,DNS:chunkserver-2,DNS:chunkserver-3,IP:127.0.0.1
EOF

cat > "$TMP/client.ext" <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth
subjectAltName=DNS:gfs-client,DNS:localhost
EOF

# CA
openssl genrsa -out "$OUT/ca.key" 4096 >/dev/null 2>&1
openssl req -x509 -new -nodes -key "$OUT/ca.key" -sha256 -days 3650 \
  -subj "/CN=gofs-ca" -out "$OUT/ca.crt"

# Server certificate (same identity is used by master and chunkservers; dial
# targets must resolve to one of the SANs above unless --tls-server-name is set).
openssl genrsa -out "$OUT/server.key" 4096 >/dev/null 2>&1
openssl req -new -key "$OUT/server.key" -subj "/CN=gfs-server" -out "$TMP/server.csr"
openssl x509 -req -in "$TMP/server.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
  -CAcreateserial -out "$OUT/server.crt" -days 3650 -sha256 -extfile "$TMP/server.ext"

# Client certificate for mTLS between services.
openssl genrsa -out "$OUT/client.key" 4096 >/dev/null 2>&1
openssl req -new -key "$OUT/client.key" -subj "/CN=gfs-client" -out "$TMP/client.csr"
openssl x509 -req -in "$TMP/client.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
  -CAcreateserial -out "$OUT/client.crt" -days 3650 -sha256 -extfile "$TMP/client.ext"

chmod 600 "$OUT"/*.key
chmod 644 "$OUT"/*.crt
echo "certificates written to $OUT"
