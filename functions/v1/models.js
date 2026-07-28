export async function onRequestGet(context) {
  const { request, env } = context;
  const backendUrl = `${env.RENDER_BACKEND_URL}/v1/models`;

  const response = await fetch(backendUrl, {
    headers: { "X-Internal-Secret": env.INTERNAL_SECRET || "" }
  });

  return new Response(await response.text(), {
    status: response.status,
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
  });
}
