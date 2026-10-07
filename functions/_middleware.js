export async function onRequest(context) {
  // Handle CORS preflight across all Pages functions
  if (context.request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key, anthropic-version, X-Model-Name, X-Internal-Secret, *",
        "Access-Control-Max-Age": "86400",
      },
    });
  }

  const response = await context.next();

  // Ensure Access-Control-Allow-Origin: * is present on all outgoing responses
  if (!response.headers.has("Access-Control-Allow-Origin")) {
    const headers = new Headers(response.headers);
    headers.set("Access-Control-Allow-Origin", "*");
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: headers,
    });
  }

  return response;
}
