#!/usr/bin/env python3
import requests
import json

# Let's inspect what opencode.ai returns to understand the 400 and 401 errors
url = "http://127.0.0.1:8794/v1/chat/completions"
headers = {
    "Authorization": "Bearer test-key",
    "Content-Type": "application/json",
    "x-opencode-session": "ses_f1ca452fdffe1IvfaQCvkIzXHe",
    "x-opencode-request": "msg_f0f66a50bffeB7MDxc270HJi55"
}
payload = {
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Count from 1 to 5. Be very brief."}],
    "stream": True
}

print("Sending request to proxy...")
try:
    r = requests.post(url, json=payload, headers=headers, stream=True, timeout=60)
    print("Status code:", r.status_code)
    print("Headers:", dict(r.headers))
    for line in r.iter_lines():
        if line:
            print("Line:", line.decode("utf-8")[:100])
except Exception as e:
    print("Error:", e)
