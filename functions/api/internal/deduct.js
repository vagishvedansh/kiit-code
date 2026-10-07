export async function onRequestPost(context) {
  const { request, env = {} } = context;

  const secret = request.headers.get("X-Internal-Secret");
  if (secret !== (env.INTERNAL_SECRET || "kiit_proxy_sec_998877")) {
    return new Response(JSON.stringify({ error: "Forbidden" }), {
      status: 403,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
  }

  try {
    const body = await request.json();
    const { key_value, model, prompt_tokens, completion_tokens, cost } = body;

    if (!env.DB || typeof env.DB.prepare !== "function") {
      return new Response(JSON.stringify({ success: true, mocked: true }), {
        status: 200,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    const keyRecord = await env.DB.prepare(
      `SELECT id, user_id FROM api_keys WHERE key_value = ?`
    ).bind(key_value).first();

    if (!keyRecord) {
      return new Response(JSON.stringify({ error: "Key not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
      });
    }

    await env.DB.batch([
      env.DB.prepare(
        `UPDATE users SET credit_balance = credit_balance - ? WHERE id = ?`
      ).bind(cost, keyRecord.user_id),
      env.DB.prepare(
        `INSERT INTO usage_logs (id, api_key_id, model, prompt_tokens, completion_tokens, cost_deducted)
         VALUES (?, ?, ?, ?, ?, ?)`
      ).bind(crypto.randomUUID(), keyRecord.id, model, prompt_tokens, completion_tokens, cost)
    ]);

    return new Response(JSON.stringify({ success: true }), {
      status: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
  }
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, X-Internal-Secret, *",
      "Access-Control-Max-Age": "86400",
    }
  });
}
