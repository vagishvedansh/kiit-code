export async function onRequestGet(context) {
  const { request, env = {} } = context;
  const backendBase = env.RENDER_BACKEND_URL || env.BACKEND_URL || "http://127.0.0.1:8080";
  const backendUrl = `${backendBase}/v1/models`;

  try {
    const response = await fetch(backendUrl, {
      headers: { "X-Internal-Secret": env.INTERNAL_SECRET || "kiit_proxy_sec_998877" }
    });

    const bodyText = await response.text();
    return new Response(bodyText, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("Content-Type") || "application/json",
        "Access-Control-Allow-Origin": "*",
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: "Backend unreachable" }), {
      status: 502,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      }
    });
  }
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key, *",
      "Access-Control-Max-Age": "86400",
    }
  });
}
