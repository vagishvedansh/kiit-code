export async function onRequestGet(context) {
  const renderUrl = "https://kiitcode.onrender.com/v1/models";
  
  const response = await fetch(renderUrl, {
    method: "GET",
    headers: {
      "X-Internal-Secret": context.env.INTERNAL_SECRET || ""
    }
  });

  return new Response(await response.text(), {
    status: response.status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    }
  });
}
