export async function onRequestPost(context) {
  const { request, env } = context;

  const backendUrl = `${env.RENDER_BACKEND_URL}/v1/chat/completions`;
  const proxyHeaders = new Headers(request.headers);
  proxyHeaders.set("X-Internal-Secret", env.INTERNAL_SECRET || "");

  const modelCodes = {
    "op5": "claude-opus-5", "sn5": "claude-sonnet-5", "fb5": "claude-fable-5",
    "th8": "claude-4.8-thinking", "g56": "gpt-5.6-sol", "qw3": "qwen-3.6-coder"
  };

  let bodyToSend;
  try {
    const parsedBody = await request.json();
    let modelName = parsedBody.model || "";
    modelName = modelCodes[modelName] || modelName;
    if (modelName) {
      proxyHeaders.set("X-Model-Name", modelName);
      parsedBody.model = "default";
    }
    bodyToSend = JSON.stringify(parsedBody);
  } catch (_) {
    bodyToSend = "{}";
  }

  try {
    const renderResponse = await fetch(backendUrl, {
      method: "POST",
      headers: proxyHeaders,
      body: bodyToSend,
    });

    return new Response(await renderResponse.text(), {
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
