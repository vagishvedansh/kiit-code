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

  let user;
  try {
    user = await env.DB.prepare(
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
  proxyHeaders.set("X-Internal-Secret", env.INTERNAL_SECRET || "");

  const modelCodes = {
    "op5": "claude-opus-5", "sn5": "claude-sonnet-5", "fb5": "claude-fable-5",
    "th8": "claude-4.8-thinking", "g56": "gpt-5.6-sol", "qw3": "qwen-3.6-coder"
  };

  let bodyToSend = "{}";
  try {
    const parsedBody = await request.json();
    let modelName = parsedBody.model || "";
    modelName = modelCodes[modelName] || modelName;
    if (modelName) {
      proxyHeaders.set("X-Model-Name", modelName);
      parsedBody.model = "default";
    }
    bodyToSend = JSON.stringify(parsedBody);
  } catch (_) {}

  try {
    const renderResponse = await fetch(backendUrl, {
      method: "POST",
      headers: proxyHeaders,
      body: bodyToSend,
    });

    const responseData = await renderResponse.json();
    const usage = responseData.usage || {};
    const promptTokens = usage.prompt_tokens || 0;
    const completionTokens = usage.completion_tokens || 0;
    const totalTokens = usage.total_tokens || (promptTokens + completionTokens);
    const costDeducted = totalTokens * 0.000001;

    if (totalTokens > 0) {
      const logId = Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
      try {
        await env.DB.prepare(
          `UPDATE users SET credit_balance = credit_balance - ? WHERE id = (SELECT user_id FROM api_keys WHERE id = ?)`
        ).bind(costDeducted, user.key_id).run();

        await env.DB.prepare(
          `INSERT INTO usage_logs (id, api_key_id, model, prompt_tokens, completion_tokens, cost_deducted) VALUES (?, ?, ?, ?, ?, ?)`
        ).bind(logId, user.key_id, responseData.model || "unknown", promptTokens, completionTokens, costDeducted).run();
      } catch (billingErr) {
        console.error("Billing error:", billingErr.message);
      }
    }

    return new Response(JSON.stringify(responseData), {
      status: renderResponse.status,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    return new Response(JSON.stringify({ error: "Backend unreachable" }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
}
