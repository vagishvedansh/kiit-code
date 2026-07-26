import os
import httpx
from fastapi import FastAPI, Request, Response, HTTPException

app = FastAPI()

INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")
CF_PAGES_URL = os.getenv("CF_PAGES_URL", "")

# SOCKS5 proxy pointing to local Tor process running in container
TOR_PROXY = "socks5://127.0.0.1:9050"

@app.get("/")
async def health():
    return {"status": "running", "service": "kiit-code backend"}

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request):
    # Verify secret header from Cloudflare Pages
    incoming_secret = request.headers.get("X-Internal-Secret")
    if INTERNAL_SECRET and incoming_secret != INTERNAL_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized request source")

    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)

    # Route request through Tor to target LLM provider (e.g. OpenRouter / Nvidia NIM)
    async with httpx.AsyncClient(proxies=TOR_PROXY, timeout=120.0) as client:
        target_url = "https://openrouter.ai/api/v1/chat/completions"
        resp = await client.post(target_url, content=body, headers=headers)
        return Response(content=resp.content, status_code=resp.status_code, headers=dict(resp.headers))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8767))
    uvicorn.run(app, host="0.0.0.0", port=port)
