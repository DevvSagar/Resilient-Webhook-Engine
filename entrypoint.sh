#!/bin/sh
set -e

echo "================================================================================"
echo "🚀 STARTING RELAYHOOK CONTAINERIZED RUNTIME"
echo "================================================================================"

echo "⏳ Step 1/3: Checking database and cache availability..."
python - << 'EOF'
import sys
import time
import os
import socket

postgres_host = os.getenv("POSTGRES_SERVER", "postgres")
postgres_port = int(os.getenv("POSTGRES_PORT", 5432))
redis_host = os.getenv("REDIS_HOST", "redis")
redis_port = int(os.getenv("REDIS_PORT", 6379))

deadline = time.time() + 45

def wait_for_service(host, port, name):
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"✅ Connected to {name} at {host}:{port}")
                return True
        except OSError:
            time.sleep(1)
    print(f"❌ Timed out waiting for {name} at {host}:{port}")
    return False

if not wait_for_service(postgres_host, postgres_port, "PostgreSQL"):
    sys.exit(1)

if not wait_for_service(redis_host, redis_port, "Redis"):
    sys.exit(1)
EOF

echo "🔄 Step 2/3: Applying database migrations (alembic upgrade head)..."
alembic upgrade head

echo "⚡ Step 3/3: Launching production Uvicorn server on port 8000..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
