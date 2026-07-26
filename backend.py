import os
import httpx
from fastapi import FastAPI, Request, Response, HTTPException

app = FastAPI()

INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")
OPENCODE_URL = "https://opencode.ai/zen/go/v1/chat/completions"
TOR_PROXY = "socks5://127.0.0.1:9050"

@app.get("/")
async def health():
    return {"status": "running", "service": "kiit-code backend"}

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request):
    incoming_secret = request.headers.get("X-Internal-Secret")
    if INTERNAL_SECRET and incoming_secret != INTERNAL_SECRET:
        raise HTTPException(status_code=403, detail="Unauthorized request source")

    body = await request.body()
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "OpenCode-Agent/1.0"
    }

    try:
        async with httpx.AsyncClient(proxy=TOR_PROXY, timeout=120.0) as client:
            resp = await client.post(OPENCODE_URL, content=body, headers=headers)
            return Response(
                content=resp.content, 
                status_code=resp.status_code, 
                headers={"Content-Type": "application/json"}
            )
    except Exception as tor_err:
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(OPENCODE_URL, content=body, headers=headers)
                return Response(
                    content=resp.content, 
                    status_code=resp.status_code, 
                    headers={"Content-Type": "application/json"}
                )
        except Exception as fallback_err:
            raise HTTPException(
                status_code=502, 
                detail=f"Upstream provider error: {str(fallback_err)}"
            )
