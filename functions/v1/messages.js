export async function onRequestPost(context) {
  const { request, env } = context;

  let apiKey = request.headers.get("x-api-key") || "";
  if (!apiKey) {
    const authHeader = request.headers.get("Authorization") || "";
    apiKey = authHeader.replace("Bearer ", "").trim();
  }

  if (!apiKey) {
    return new Response(JSON.stringify({
      type: "error",
      error: { type: "authentication_error", message: "Missing API Key" }
    }), {
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
      return new Response(JSON.stringify({
        type: "error",
        error: { type: "authentication_error", message: "Invalid or disabled API Key" }
      }), {
        status: 401,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (user.credit_balance <= 0) {
      return new Response(JSON.stringify({
        type: "error",
        error: { type: "invalid_request_error", message: "Credit balance exhausted ($0.00 remaining)." }
      }), {
        status: 402,
        headers: { "Content-Type": "application/json" }
      });
    }
  } catch (err) {
    return new Response(JSON.stringify({
      type: "error",
      error: { type: "api_error", message: "Database error during key verification" }
    }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    });
  }

  const backendUrl = `${env.RENDER_BACKEND_URL}/v1/messages`;

  const proxyHeaders = new Headers(request.headers);
  proxyHeaders.set("X-Internal-Secret", env.INTERNAL_SECRET || "");
  proxyHeaders.delete("host");
  proxyHeaders.delete("content-length");

  const modelCodes = {
    "g54": "gpt-4o-mini", "g4o": "gpt-4o", "g4m": "gpt-4o-mini",
    "dsr": "deepseek-r1", "dsp": "deepseek-pro", "dsv": "deepseek-v3",
    "qw3": "qwen-3.6-coder", "qw2": "qwen-2.5-coder", "km2": "kimi-k2.6", "mm2": "minimax-m2.7"
  };

  let bodyToSend = "{}";
  try {
    const parsedBody = await request.json();
    let modelName = parsedBody.model || "";
    modelName = modelCodes[modelName] || modelName;
    if (modelName) {
      proxyHeaders.set("X-Model-Name", modelName);
    }
    bodyToSend = JSON.stringify(parsedBody);
  } catch (_) {}

  try {
    const renderResponse = await fetch(backendUrl, {
      method: "POST",
      headers: proxyHeaders,
      body: bodyToSend,
    });

    if (!renderResponse.ok) {
      const errData = await renderResponse.text();
      return new Response(errData, {
        status: renderResponse.status,
        headers: { "Content-Type": "application/json" }
      });
    }

    const contentType = renderResponse.headers.get("Content-Type") || "";
    if (contentType.includes("text/event-stream")) {
      return new Response(renderResponse.body, {
        status: renderResponse.status,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          "Access-Control-Allow-Origin": "*",
          "anthropic-version": "2023-06-01"
        }
      });
    }

    const responseData = await renderResponse.json();
    const usage = responseData.usage || {};
    const promptTokens = usage.input_tokens || usage.prompt_tokens || 0;
    const completionTokens = usage.output_tokens || usage.completion_tokens || 0;
    const totalTokens = promptTokens + completionTokens;

    context.waitUntil(
      (async () => {
        try {
          const costPer1k = 0.0015;
          const cost = (totalTokens / 1000) * costPer1k;

          await env.DB.prepare(
            `UPDATE users SET credit_balance = credit_balance - ? WHERE id = (SELECT user_id FROM api_keys WHERE id = ?)`
          ).bind(cost, user.key_id).run();

          await env.DB.prepare(
            `INSERT INTO usage_logs (key_id, prompt_tokens, completion_tokens, total_tokens, cost) VALUES (?, ?, ?, ?, ?)`
          ).bind(user.key_id, promptTokens, completionTokens, totalTokens, cost).run();
        } catch (e) {
          console.error("Failed to log usage or update credit balance:", e);
        }
      })()
    );

    return new Response(JSON.stringify(responseData), {
      status: renderResponse.status,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "anthropic-version": "2023-06-01"
      }
    });
  } catch (err) {
    return new Response(JSON.stringify({
      type: "error",
      error: { type: "api_error", message: `Upstream gateway error: ${err.message}` }
    }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "*",
      "Access-Control-Max-Age": "86400"
    }
  });
}
