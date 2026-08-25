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
  proxyHeaders.set("Accept", "application/json");
  proxyHeaders.set("Content-Type", "application/json");
  proxyHeaders.delete("host");
  proxyHeaders.delete("content-length");

  const modelCodes = {
    "g54": "gpt-4o-mini", "g4o": "gpt-4o", "g4m": "gpt-4o-mini",
    "dsr": "deepseek-r1", "dsv": "deepseek-v3",
    "qw3": "qwen-3.6-coder", "qw2": "qwen-2.5-coder", "km2": "kimi-k2.6", "mm2": "minimax-m2.7"
  };

  let isStream = false;
  let modelName = "gpt-4o";
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
      parsedBody.stream = false;
      bodyToSend = JSON.stringify(parsedBody);
    }
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
    if (responseData.choices && Array.isArray(responseData.choices)) {
      for (const choice of responseData.choices) {
        if (choice.message && typeof choice.message.content === "string") {
          extractedText += choice.message.content;
        }
      }
    }

    if (responseData.content && Array.isArray(responseData.content)) {
      for (const item of responseData.content) {
        if (item.type === "text" && typeof item.text === "string") {
          extractedText += item.text;
        }
      }
      delete responseData.content;
    }

    extractedText = sanitizeModelText(extractedText, modelName);
    responseData.choices = [
      {
        index: 0,
        message: {
          role: "assistant",
          content: extractedText
        },
        finish_reason: "stop"
      }
    ];

    if (modelName) {
      responseData.model = modelName;
    }
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

    if (isStream) {
      const completionId = responseData.id || ("chatcmpl-" + Math.random().toString(36).slice(2, 14));
      const modelOut = responseData.model || modelName || "gpt-4o";
      const parts = extractedText.match(/\S+\s*|\s+/g) || [extractedText];

      const stream = new ReadableStream({
        async start(controller) {
          const encoder = new TextEncoder();
          for (const p of parts) {
            if (!p) continue;
            const chunkPayload = JSON.stringify({
              id: completionId,
              object: "chat.completion.chunk",
              created: Math.floor(Date.now() / 1000),
              model: modelOut,
              choices: [{ index: 0, delta: { content: p, role: "assistant" }, finish_reason: null }]
            });
            controller.enqueue(encoder.encode(`data: ${chunkPayload}\n\n`));
          }

          const finalPayload = JSON.stringify({
            id: completionId,
            object: "chat.completion.chunk",
            created: Math.floor(Date.now() / 1000),
            model: modelOut,
            choices: [{ index: 0, delta: {}, finish_reason: "stop" }]
          });
          controller.enqueue(encoder.encode(`data: ${finalPayload}\n\n`));
          controller.enqueue(encoder.encode(`data: [DONE]\n\n`));
          controller.close();
        }
      });

      return new Response(stream, {
        status: 200,
        headers: {
          "Content-Type": "text/event-stream",
          "Cache-Control": "no-cache",
          "Access-Control-Allow-Origin": "*"
        }
      });
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
  return "GPT-4o";
}

function vendorFor(model) {
  const m = (model || "").toLowerCase();
  if (/claude|opus|sonnet|haiku/i.test(m)) return "Anthropic";
  if (/gpt/i.test(m)) return "OpenAI";
  if (/deepseek/i.test(m)) return "DeepSeek";
  if (/qwen/i.test(m)) return "Alibaba Cloud";
  if (/kimi/i.test(m)) return "Moonshot AI";
  if (/minimax/i.test(m)) return "MiniMax";
  return "OpenAI";
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
  return fixMissingSpaces(clean);
}

function fixMissingSpaces(text) {
  if (!text) return text;
  let fixed = text;
  fixed = fixed.replace(/([a-z0-9])([A-Z])/g, "$1 $2");
  fixed = fixed.replace(/([.,!?:;])([A-Za-z0-9])/g, "$1 $2");
  fixed = fixed.replace(/([a-z])([0-9])/g, "$1 $2");
  fixed = fixed.replace(/([0-9])([a-zA-Z])/g, "$1 $2");

  for (let pass = 0; pass < 2; pass++) {
    fixed = fixed.replace(/(a)(large)/gi, "$1 $2");
    fixed = fixed.replace(/(large)(language)(model)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(language)(model)/gi, "$1 $2");
    fixed = fixed.replace(/(model)(created|developed|trained|designed|assisted)/gi, "$1 $2");
    fixed = fixed.replace(/(created|developed|trained|designed|assisted)(by|for|to)/gi, "$1 $2");
    fixed = fixed.replace(/(by|for|to)(Anthropic|OpenAI)/gi, "$1 $2");
    fixed = fixed.replace(/(designed)(to)(be)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(to)(be)(helpful|honest|harmless|thoughtful|engaging)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(and)(honest|harmless|helpful|truthful|thoughtful|engaging|more)/gi, "$1 $2");
    fixed = fixed.replace(/(engaging)(across)/gi, "$1 $2");
    fixed = fixed.replace(/(across)(a)(wide)(range)(of)(topics)/gi, "$1 $2 $3 $4 $5 $6");
    fixed = fixed.replace(/(wide)(range)(of)(topics|tasks)/gi, "$1 $2 $3 $4");
    fixed = fixed.replace(/(range)(of)(topics|tasks)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(of)(topics|tasks)/gi, "$1 $2");
    fixed = fixed.replace(/(here)(to)(help)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(help)(you)(with|today)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(with)(questions|tasks|writing|coding|analysis|whatever|a)/gi, "$1 $2");
    fixed = fixed.replace(/(how)(can)(i)(help)/gi, "$1 $2 $3 $4");
    fixed = fixed.replace(/(happy)(to)(help)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(whatever)(you)(need)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(an)(AI)(assistant)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(AI)(assistant)/gi, "$1 $2");
    fixed = fixed.replace(/(Claude)(3)(Opus|Sonnet|Haiku)/gi, "$1 $2 $3");
    fixed = fixed.replace(/(Claude)(Opus|Sonnet|Haiku)/gi, "$1 $2");
  }

  fixed = fixed.replace(/\s+/g, " ");
  return fixed.trim();
}
