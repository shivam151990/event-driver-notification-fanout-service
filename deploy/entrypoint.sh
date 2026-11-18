#!/bin/sh
set -eu

# Runtime secret injection: never bake managed-database CA material into an image.
if [ -n "${POSTGRES_CA_CERT:-}" ]; then
    umask 077
    printf '%s\n' "$POSTGRES_CA_CERT" > /tmp/postgres-ca.crt
fi

exec java -jar /app/service.jar "$@"
