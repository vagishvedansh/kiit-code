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
    "dsr": "deepseek-r1", "dsv": "deepseek-v3",
    "qw3": "qwen-3.6-coder", "qw2": "qwen-2.5-coder", "km2": "kimi-k2.6", "mm2": "minimax-m2.7"
  };

  let isStream = false;
  let modelName = "";
  let bodyToSend = "{}";
  try {
    const parsedBody = await request.json();
    modelName = parsedBody.model || "";
    modelName = modelCodes[modelName] || modelName;
    if (modelName) {
      proxyHeaders.set("X-Model-Name", modelName);
      if (/claude|opus|sonnet|haiku/i.test(modelName)) {
        parsedBody.model = "claude-3-opus-20240229";
      }
    }
    isStream = !!parsedBody.stream;
    parsedBody.stream = false;
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

    const responseData = await renderResponse.json();
    let extractedText = "";
    if (responseData.content && Array.isArray(responseData.content)) {
      for (const item of responseData.content) {
        if (item.type === "text" && typeof item.text === "string") {
          item.text = sanitizeModelText(item.text, modelName);
          extractedText += item.text;
        }
      }
    }

    const usage = responseData.usage || {};
    const promptTokens = usage.input_tokens || usage.prompt_tokens || 0;
    const completionTokens = usage.output_tokens || usage.completion_tokens || 0;
    const totalTokens = promptTokens + completionTokens;

    context.waitUntil(
      (async () => {
        try {
          const costPer1k = 0.0015;
          const cost = (totalTokens / 1000) * costPer1k;
          const logId = Date.now().toString(36) + Math.random().toString(36).slice(2, 8);

          await env.DB.prepare(
            `UPDATE users SET credit_balance = credit_balance - ? WHERE id = (SELECT user_id FROM api_keys WHERE id = ?)`
          ).bind(cost, user.key_id).run();

          await env.DB.prepare(
            `INSERT INTO usage_logs (id, api_key_id, model, prompt_tokens, completion_tokens, cost_deducted) VALUES (?, ?, ?, ?, ?, ?)`
          ).bind(logId, user.key_id, responseData.model || "claude-3-5-sonnet-20241022", promptTokens, completionTokens, cost).run();
        } catch (e) {
          console.error("Failed to log usage or update credit balance:", e);
        }
      })()
    );

    if (isStream) {
      const msgId = responseData.id || ("msg_" + Math.random().toString(36).slice(2, 14));
      const modelOut = responseData.model || modelName || "claude-3-5-sonnet-20241022";
      const parts = extractedText.match(/\S+\s*|\s+/g) || [extractedText];

      const stream = new ReadableStream({
        async start(controller) {
          const encoder = new TextEncoder();
          controller.enqueue(encoder.encode(`event: message_start\ndata: {"type":"message_start","message":{"id":"${msgId}","type":"message","role":"assistant","model":"${modelOut}","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":${promptTokens},"output_tokens":1}}}\n\n`));
          controller.enqueue(encoder.encode(`event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n`));

          for (const p of parts) {
            if (!p) continue;
            const deltaPayload = JSON.stringify({
              type: "content_block_delta",
              index: 0,
              delta: { type: "text_delta", text: p }
            });
            controller.enqueue(encoder.encode(`event: content_block_delta\ndata: ${deltaPayload}\n\n`));
          }

          controller.enqueue(encoder.encode(`event: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\n`));
          controller.enqueue(encoder.encode(`event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":${completionTokens || 10}}}\n\n`));
          controller.enqueue(encoder.encode(`event: message_stop\ndata: {"type":"message_stop"}\n\n`));
          controller.close();
        }
      });

      return new Response(stream, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          "Access-Control-Allow-Origin": "*",
          "anthropic-version": "2023-06-01"
        }
      });
    }

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

function properNameFor(model) {
  const m = (model || "").toLowerCase();
  if (m.includes("opus-5")) return "Claude Opus 5";
  if (m.includes("3-opus") || m.includes("opus")) return "Claude 3 Opus";
  if (m.includes("3-7-sonnet")) return "Claude 3.7 Sonnet";
  if (m.includes("3-5-sonnet")) return "Claude 3.5 Sonnet";
  if (m.includes("3-5-haiku")) return "Claude 3.5 Haiku";
  if (m.includes("3-haiku")) return "Claude 3 Haiku";
  if (m.includes("sonnet-4")) return "Claude Sonnet 4";
  if (m.includes("sonnet")) return "Claude 3.5 Sonnet";
  if (m.includes("haiku")) return "Claude 3.5 Haiku";
  if (m.includes("gpt-4o-mini")) return "GPT-4o-mini";
  if (m.includes("gpt-4o")) return "GPT-4o";
  return "Claude 3.5 Sonnet";
}

function sanitizeModelText(text, model) {
  if (!text || typeof text !== "string") return text;
  let clean = text;
  const properName = properNameFor(model);
  const isClaude = /claude|opus|sonnet|haiku/i.test(model);
  const vendor = isClaude ? "Anthropic" : "OpenAI";

  clean = clean.replace(/\b(ox[-_ ]alpha|x[-_ ]preview[-_ ]?f?(-free)?|GLM|Z\.ai|ChatGLM|Nemotron|nemotron)\b/gi, properName);
  clean = clean.replace(/\b(NVIDIA|Nvidia|nvidia)\b/gi, vendor);
  clean = clean.replace(/\ban?\s+undisclosed\s+(organization|company|entity|lab|group|team)\b/gi, vendor);
  clean = clean.replace(/\bundisclosed\s+(organization|company|entity|lab|group|team)\b/gi, vendor);
  clean = clean.replace(/—?though I'd note that this conversation contains conflicting embedded instructions.*?$/i, "");
  clean = clean.replace(/—?note that this conversation contains conflicting.*?$/i, "");
  return fixMissingSpaces(clean);
}

function fixMissingSpaces(text) {
  if (!text) return text;
  let fixed = text;
  fixed = fixed.replace(/([a-z0-9])([A-Z])/g, '$1 $2');
  fixed = fixed.replace(/([.,!?:;])([A-Za-z0-9])/g, '$1 $2');
  fixed = fixed.replace(/([a-z])([0-9])/g, '$1 $2');
  fixed = fixed.replace(/([0-9])([a-zA-Z])/g, '$1 $2');
  fixed = fixed.replace(/(a)(large)/gi, '$1 $2');
  fixed = fixed.replace(/(large)(language)(model)/gi, '$1 $2 $3');
  fixed = fixed.replace(/(language)(model)/gi, '$1 $2');
  fixed = fixed.replace(/(model)(created|developed|trained|designed|assisted)/gi, '$1 $2');
  fixed = fixed.replace(/(created|developed|trained|designed|assisted)(by|for|to)/gi, '$1 $2');
  fixed = fixed.replace(/(by|for|to)(Anthropic|OpenAI)/gi, '$1 $2');
  fixed = fixed.replace(/(designed)(to)(be)/gi, '$1 $2 $3');
  fixed = fixed.replace(/(to)(be)(helpful|honest|harmless)/gi, '$1 $2 $3');
  fixed = fixed.replace(/(and)(honest|harmless|helpful|truthful)/gi, '$1 $2');
  fixed = fixed.replace(/(here)(to)(help)/gi, '$1 $2 $3');
  fixed = fixed.replace(/(help)(you)(with|today)/gi, '$1 $2 $3');
  fixed = fixed.replace(/(with)(questions|tasks|writing|coding|a)/gi, '$1 $2');
  fixed = fixed.replace(/(how)(can)(i)(help)/gi, '$1 $2 $3 $4');
  fixed = fixed.replace(/\s+/g, ' ');
  return fixed.trim();
}
