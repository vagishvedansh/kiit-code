export async function onRequest(context) {
  return new Response(`
    <!DOCTYPE html>
    <html>
      <head>
        <title>KIIT Code Gateway</title>
        <style>
          body { font-family: monospace; background: #0d1117; color: #58a6ff; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
          .card { border: 1px solid #30363d; padding: 2rem; border-radius: 8px; text-align: center; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>⚡ KIIT Code API Gateway</h1>
          <p>Status: <span style="color:#3fb950;">ONLINE</span></p>
          <p>Base URL: <code>https://kiitcode.pages.dev/v1</code></p>
        </div>
      </body>
    </html>
  `, {
    headers: { "Content-Type": "text/html" }
  });
}
