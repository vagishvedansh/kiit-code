export async function onRequestPost(context) {
  const { request, env } = context;

  const secret = request.headers.get("X-Internal-Secret");
  if (secret !== env.INTERNAL_SECRET) {
    return new Response(JSON.stringify({ error: "Forbidden" }), { status: 403 });
  }

  try {
    const body = await request.json();
    const { key_value, model, prompt_tokens, completion_tokens, cost } = body;

    const keyRecord = await env.DB.prepare(
      `SELECT id, user_id FROM api_keys WHERE key_value = ?`
    ).bind(key_value).first();

    if (!keyRecord) {
      return new Response(JSON.stringify({ error: "Key not found" }), { status: 404 });
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

    return new Response(JSON.stringify({ success: true }), { status: 200 });
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), { status: 500 });
  }
}
