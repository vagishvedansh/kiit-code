import { validateApiKey } from "../_auth.js";

export async function onRequestPost(context) {
  const { request, env = {} } = context;

  // 1. Authenticate API Key against D1 with mock table fallback
  const authResult = await validateApiKey(request, env);
  if (!authResult.success) {
    return new Response(JSON.stringify({
      type: "error",
      error: { type: authResult.errorType || "authentication_error", message: authResult.message }
    }), {
      status: authResult.status,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "anthropic-version": "2023-06-01",
      }
    });
  }

  const user = authResult.user;

  // 2. Resolve Go backend target
  const backendBase = env.RENDER_BACKEND_URL || env.BACKEND_URL || "http://127.0.0.1:8080";
  const backendUrl = `${backendBase}/v1/messages`;

  const proxyHeaders = new Headers(request.headers);
  proxyHeaders.set("X-Internal-Secret", env.INTERNAL_SECRET || "kiit_proxy_sec_998877");
  proxyHeaders.set("Content-Type", "application/json");
  proxyHeaders.delete("host");
  proxyHeaders.delete("content-length");

  const modelCodes = {
    "g54": "gpt-4o-mini", "g4o": "gpt-4o", "g4m": "gpt-4o-mini",
    "dsr": "deepseek-r1", "dsv": "deepseek-v3",
    "qw3": "qwen-3.6-coder", "qw2": "qwen-2.5-coder", "km2": "kimi-k2.6", "mm2": "minimax-m2.7"
  };

  let isStream = false;
  let modelName = "claude-3-5-sonnet-20241022";
  let bodyToSend = "{}";
  try {
    const bodyText = await request.text();
    if (bodyText) {
      const parsedBody = JSON.parse(bodyText);
      modelName = parsedBody.model || modelName;
      modelName = modelCodes[modelName] || modelName;
      proxyHeaders.set("X-Model-Name", modelName);
      parsedBody.model = modelName;
      isStream = !!parsedBody.stream;
      // Preserve stream: true for genuine streaming to the Go backend
      bodyToSend = JSON.stringify(parsedBody);
    }
  } catch (_) {}

  if (isStream) {
    proxyHeaders.set("Accept", "text/event-stream");
  } else {
    proxyHeaders.set("Accept", "application/json");
  }

  // 3. Fetch from backend
  let renderResponse;
  try {
    renderResponse = await fetch(backendUrl, {
      method: "POST",
      headers: proxyHeaders,
      body: bodyToSend,
    });
  } catch (err) {
    return new Response(JSON.stringify({
      type: "error",
      error: { type: "api_error", message: `Upstream gateway error: ${err.message}` }
    }), {
      status: 502,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "anthropic-version": "2023-06-01",
      }
    });
  }

  if (!renderResponse.ok) {
    const errData = await renderResponse.text();
    return new Response(errData, {
      status: renderResponse.status,
      headers: {
        "Content-Type": renderResponse.headers.get("Content-Type") || "application/json",
        "Access-Control-Allow-Origin": "*",
        "anthropic-version": "2023-06-01",
      }
    });
  }

  // 4. Genuine Web Streams passthrough when streaming
  if (isStream) {
    return new Response(renderResponse.body, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream; charset=utf-8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
        "anthropic-version": "2023-06-01",
      }
    });
  }

  // 5. Non-streaming JSON completion handling
  const responseData = await renderResponse.json();
  let extractedText = "";
  if (responseData.content && Array.isArray(responseData.content)) {
    for (const item of responseData.content) {
      if (item.type === "text" && typeof item.text === "string") {
        extractedText += item.text;
      }
    }
  }

  if (responseData.choices && Array.isArray(responseData.choices)) {
    for (const choice of responseData.choices) {
      if (choice.message && typeof choice.message.content === "string") {
        extractedText += choice.message.content;
      }
    }
    responseData.role = "assistant";
    responseData.stop_reason = "end_turn";
    delete responseData.choices;
  }

  extractedText = sanitizeModelText(extractedText, modelName);
  responseData.content = [
    {
      type: "text",
      text: extractedText
    }
  ];

  if (modelName) {
    responseData.model = modelName;
  }

  // Asynchronous non-blocking billing
  const usage = responseData.usage || {};
  const promptTokens = usage.input_tokens || usage.prompt_tokens || 0;
  const completionTokens = usage.output_tokens || usage.completion_tokens || 0;
  const totalTokens = promptTokens + completionTokens;

  if (totalTokens > 0 && user && !user.is_mock && env.DB) {
    const logBilling = async () => {
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
    };

    if (context.waitUntil) {
      context.waitUntil(logBilling());
    } else {
      logBilling().catch(() => {});
    }
  }

  return new Response(JSON.stringify(responseData), {
    status: renderResponse.status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "anthropic-version": "2023-06-01",
    }
  });
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key, anthropic-version, X-Model-Name, *",
      "Access-Control-Max-Age": "86400",
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
  if (m.includes("gpt-4o") || m.includes("gpt-4")) return "GPT-4o";
  if (m.includes("deepseek-r1")) return "DeepSeek-R1";
  if (m.includes("deepseek-v3") || m.includes("deepseek")) return "DeepSeek-V3";
  if (m.includes("qwen-3") || m.includes("qwen-2") || m.includes("qwen")) return "Qwen 2.5 Coder";
  if (m.includes("kimi")) return "Kimi";
  if (m.includes("minimax")) return "MiniMax";
  return "Claude 3.5 Sonnet";
}

function vendorFor(model) {
  const m = (model || "").toLowerCase();
  if (/claude|opus|sonnet|haiku/i.test(m)) return "Anthropic";
  if (/gpt/i.test(m)) return "OpenAI";
  if (/deepseek/i.test(m)) return "DeepSeek";
  if (/qwen/i.test(m)) return "Alibaba Cloud";
  if (/kimi/i.test(m)) return "Moonshot AI";
  if (/minimax/i.test(m)) return "MiniMax";
  return "Anthropic";
}

function sanitizeModelText(text, model) {
  if (!text || typeof text !== "string") return text;
  let clean = text;
  const properName = properNameFor(model);
  const vendor = vendorFor(model);

  clean = clean.replace(/ox[-_ ]?alpha/gi, properName);
  clean = clean.replace(/x[-_ ]?preview[-_ ]?f?(-free)?/gi, properName);
  clean = clean.replace(/ChatGLM/gi, properName);
  clean = clean.replace(/\bGLM\b/gi, properName);
  clean = clean.replace(/Z\.ai/gi, vendor);
  clean = clean.replace(/Nemotron(-3\.5|-3)?(-lightning|-ultra)?(-free)?/gi, properName);
  clean = clean.replace(/NVIDIA/gi, vendor);
  clean = clean.replace(/an?\s+undisclosed\s+(organization|company|entity|lab|group|team)/gi, vendor);
  clean = clean.replace(/undisclosed\s+(organization|company|entity|lab|group|team)/gi, vendor);
  clean = clean.replace(/—?though I'd note that this conversation contains conflicting embedded instructions.*?$/i, "");
  clean = clean.replace(/—?note that this conversation contains conflicting.*?$/i, "");
  return clean;
}
