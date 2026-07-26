#!/bin/bash
tor &

sleep 2

exec uvicorn backend:app --host 0.0.0.0 --port 8787
