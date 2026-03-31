#!/bin/sh
exec python3 -m uvicorn matching_api:app --host 0.0.0.0 --port "${PORT:-8000}"
