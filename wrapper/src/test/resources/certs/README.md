# Test-Only TLS Certificates

These certificates are **strictly for integration testing** and must NOT be used in production.

- Generated with OpenSSL as self-signed certificates
- CA CN: "Test CA - DO NOT TRUST"
- Server CN: "localhost"
- Valid for 500 years (182500 days)
- No sensitive data is protected by these certificates

The server certificate includes Subject Alternative Names (SANs) for all Docker network aliases
used by the Valkey test containers. Modern TLS clients require SANs for hostname verification.

To regenerate:

```zsh
# Generate CA key and certificate
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 182500 -key ca.key -out ca.crt \
  -subj "/C=US/ST=Test/L=Test/O=TestOnly/CN=Test CA - DO NOT TRUST"

# Generate server key and certificate
openssl genrsa -out valkey.key 2048
openssl req -new -key valkey.key -out valkey.csr \
  -subj "/C=US/ST=Test/L=Test/O=TestOnly/CN=localhost"
openssl x509 -req -days 182500 -in valkey.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out valkey.crt \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:valkey-server-address-0,DNS:valkey-server-address-1,DNS:valkey-server-address-2,DNS:valkey-server-address-3")

# Set permissions so Docker can read the key
chmod 644 valkey.key ca.key

# Clean up CSR and serial
rm valkey.csr ca.srl
```
