#!/usr/bin/env bash
# Generate a local development CA and per-node certificates for mTLS.
#
# These certificates are for local development only. They are written to
# ./certs, which is git-ignored: a private key must never be committed, and a
# committed development key has a way of becoming a production key.

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="${CERT_DIR:-$REPO_ROOT/certs}"
DAYS="${DAYS:-365}"

command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

if [ -f ca.crt ] && [ "${FORCE:-0}" != "1" ]; then
  echo "certificates already exist in $CERT_DIR (set FORCE=1 to regenerate)"
  exit 0
fi

echo "Generating a development CA in $CERT_DIR"
openssl req -x509 -newkey rsa:4096 -sha256 -days "$DAYS" -nodes \
  -keyout ca.key -out ca.crt \
  -subj "/CN=EdgeMesh Development CA/O=EdgeMesh" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null

# Every node gets both client and server usage: these are peer protocols where
# each node both dials and accepts.
gen_node() {
  local name="$1" sans="$2"
  echo "  $name"
  openssl req -newkey rsa:2048 -nodes \
    -keyout "$name.key" -out "$name.csr" \
    -subj "/CN=$name/O=EdgeMesh" 2>/dev/null

  cat > "$name.ext" <<EXT
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=$sans
EXT

  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out "$name.crt" -days "$DAYS" -sha256 -extfile "$name.ext" 2>/dev/null
  rm -f "$name.csr" "$name.ext"
  chmod 600 "$name.key"
}

for n in 1 2 3; do
  gen_node "cp-$n"   "DNS:cp-$n,DNS:localhost,IP:127.0.0.1"
  gen_node "edge-$n" "DNS:edge-$n,DNS:localhost,IP:127.0.0.1"
done

chmod 600 ca.key
rm -f ca.srl

cat <<NOTE

Development certificates written to $CERT_DIR

Enable mTLS by setting this in each node's configuration:

  security:
    enabled: true
    ca_file: $CERT_DIR/ca.crt
    cert_file: $CERT_DIR/<node-id>.crt
    key_file: $CERT_DIR/<node-id>.key

These are development credentials. Production certificates belong in a real PKI
or a secret manager, never in a repository.

NOTE
