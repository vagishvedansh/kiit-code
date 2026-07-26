import os
import json
from fastapi import FastAPI, Request, Response, HTTPException
from curl_cffi import requests as cffi_requests

app = FastAPI()

INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")
OPENCODE_URL = "https://opencode.ai/zen/v1/chat/completions"
TOR_PROXY = "socks5://127.0.0.1:9050"

VALID_MODELS = {
    "minimax-m3-free",
    "big-pickle",
    "deepseek-v4-flash-free",
    "ling-3.0-flash-free",
    "mimo-v2.5-free",
    "nemotron-3-super-free",
}

@app.get("/")
async def health():
    return {"status": "running", "service": "kiit-code backend"}

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request):
    incoming_secret = request.headers.get("X-Internal-Secret")
    if INTERNAL_SECRET and incoming_secret != INTERNAL_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized request source")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if body.get("model") not in VALID_MODELS:
        body["model"] = "minimax-m3-free"

    headers = {"Content-Type": "application/json"}

    try:
        r = cffi_requests.post(
            OPENCODE_URL,
            headers=headers,
            json=body,
            proxies={"http": TOR_PROXY, "https": TOR_PROXY},
            impersonate="chrome131",
            timeout=120,
        )
        return Response(content=r.content, status_code=r.status_code, headers={"Content-Type": "application/json"})
    except Exception as tor_err:
        try:
            r = cffi_requests.post(
                OPENCODE_URL,
                headers=headers,
                json=body,
                impersonate="chrome131",
                timeout=120,
            )
            return Response(content=r.content, status_code=r.status_code, headers={"Content-Type": "application/json"})
        except Exception as fallback_err:
            raise HTTPException(status_code=502, detail=f"Upstream provider error: {str(fallback_err)}")
