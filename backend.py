import os
import httpx
from fastapi import FastAPI, Request, Response, HTTPException

app = FastAPI()

INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
TOR_PROXY = "socks5://127.0.0.1:9050"

@app.get("/")
async def health():
    return {"status": "running", "service": "kiit-code backend"}

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request):
    incoming_secret = request.headers.get("X-Internal-Secret")

    print(f"[DEBUG] Received: '{incoming_secret}' | Expected: '{INTERNAL_SECRET}'")

    if INTERNAL_SECRET and incoming_secret != INTERNAL_SECRET:
        raise HTTPException(
            status_code=403,
            detail=f"Unauthorized: Got '{incoming_secret}', expected '{INTERNAL_SECRET}'"
        )

    body = await request.body()
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENROUTER_API_KEY}"
    }

    try:
        # Use singular 'proxy' keyword for modern httpx
        async with httpx.AsyncClient(proxy=TOR_PROXY, timeout=120.0) as client:
            resp = await client.post(
                "https://openrouter.ai/api/v1/chat/completions", 
                content=body, 
                headers=headers
            )
            return Response(content=resp.content, status_code=resp.status_code, headers={"Content-Type": "application/json"})
    except Exception as e:
        # Fallback to direct client if Tor proxy errors out
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions", 
                    content=body, 
                    headers=headers
                )
                return Response(content=resp.content, status_code=resp.status_code, headers={"Content-Type": "application/json"})
        except Exception as fallback_err:
            raise HTTPException(status_code=502, detail=f"Upstream provider error: {str(fallback_err)}")