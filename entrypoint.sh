#!/bin/sh
set -e

mkdir -p /tmp/tor_data
chmod 700 /tmp/tor_data

echo "[BOOT] Starting Tor daemon..."
tor -f /etc/tor/torrc &
TOR_PID=$!

echo "[BOOT] Waiting for Tor SOCKS listener on 127.0.0.1:9050..."
RETRIES=30
while [ $RETRIES -gt 0 ]; do
    if nc -z 127.0.0.1 9050; then
        echo "[BOOT] Tor SOCKS5 listener is UP and READY!"
        break
    fi
    sleep 0.5
    RETRIES=$((RETRIES - 1))
done

if [ $RETRIES -eq 0 ]; then
    echo "[WARN] Tor did not report ready within 15s, proceeding with server launch..."
fi

echo "[BOOT] Launching Stealth Proxy Engine..."
exec /app/server
