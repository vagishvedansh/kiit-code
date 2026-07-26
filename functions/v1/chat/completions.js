export async function onRequestPost(context) {
  const { request, env } = context;

  const authHeader = request.headers.get("Authorization") || "";
  const apiKey = authHeader.replace("Bearer ", "").trim();

  if (!apiKey) {
    return new Response(JSON.stringify({ error: "Missing API Key" }), {
      status: 401,
      headers: { "Content-Type": "application/json" }
    });
  }

  try {
    const user = await env.DB.prepare(
      `SELECT u.credit_balance, k.is_active, k.id as key_id 
       FROM api_keys k 
       JOIN users u ON k.user_id = u.id 
       WHERE k.key_value = ?`
    ).bind(apiKey).first();

    if (!user || user.is_active !== 1) {
      return new Response(JSON.stringify({ error: "Invalid or disabled API Key" }), {
        status: 401,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (user.credit_balance <= 0) {
      return new Response(JSON.stringify({ error: "Credit balance exhausted ($0.00 remaining)." }), {
        status: 402,
        headers: { "Content-Type": "application/json" }
      });
    }
  } catch (err) {
    return new Response(JSON.stringify({ error: "Database error during key verification" }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    });
  }

  const backendUrl = `${env.RENDER_BACKEND_URL}/v1/chat/completions`;

  const proxyHeaders = new Headers(request.headers);
  proxyHeaders.set("X-Kiit-Key", apiKey);

  try {
    const renderResponse = await fetch(backendUrl, {
      method: "POST",
      headers: proxyHeaders,
      body: request.body,
    });

    return new Response(renderResponse.body, {
      status: renderResponse.status,
      headers: renderResponse.headers,
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: "Failed to connect to execution proxy backend" }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
}
