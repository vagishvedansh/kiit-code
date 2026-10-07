import re

with open("main.go", "r") as f:
    code = f.read()

# Imports
code = re.sub(r'"net/http"', '"net/http"\n\tfhttp "github.com/bogdanfinn/fhttp"\n\ttls_client "github.com/bogdanfinn/tls-client"\n\t"github.com/bogdanfinn/tls-client/profiles"', code, count=1)

# sharedDirectClient
code = re.sub(r'var sharedDirectClient = &http\.Client\{[\s\S]*?\}\n', """var sharedDirectClient tls_client.HttpClient

func init() {
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(30),
		tls_client.WithClientProfile(profiles.Chrome_131),
	}
	sharedDirectClient, _ = tls_client.NewHttpClient(tls_client.NewLogger(), options...)
}
""", code)

# newTorClient
code = re.sub(r'func newTorClient\(\) \*http\.Client \{[\s\S]*?return sharedDirectClient\n\}', """func newTorClient() tls_client.HttpClient {
	proxyURLStr := os.Getenv("TOR_PROXY_URL")
	if proxyURLStr == "" {
		proxyURLStr = os.Getenv("PROXY_URL")
	}
	if proxyURLStr == "" {
		proxyURLStr = "socks5://127.0.0.1:9050"
	}

	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(35),
		tls_client.WithClientProfile(profiles.Chrome_131),
		tls_client.WithProxyUrl(proxyURLStr),
	}
	client, _ := tls_client.NewHttpClient(tls_client.NewLogger(), options...)
	return client
}""", code)

# MimoAuthenticator client
code = re.sub(r'client\s+\*http\.Client', "client tls_client.HttpClient", code)
code = re.sub(r'client:\s*&http\.Client\{[^}]+\}', 'client: nil', code)
code = re.sub(r'm\.client\.Do\(req\)', 'sharedDirectClient.Do(req)', code)


# HTTP requests and responses
code = code.replace("var resp *http.Response", "var resp *fhttp.Response")
# Be precise for http.NewRequest and http.NewRequestWithContext
code = re.sub(r'http\.NewRequest\(', 'fhttp.NewRequest(', code)
code = re.sub(r'http\.NewRequestWithContext\(', 'fhttp.NewRequestWithContext(', code)

code = re.sub(r'func newStreamClient\(\) \*http\.Client \{[\s\S]*?return &http\.Client\{\n\s*Transport: tr,\n\s*\}\n\}', """func newStreamClient() tls_client.HttpClient {
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(300),
		tls_client.WithClientProfile(profiles.Chrome_131),
	}
	client, _ := tls_client.NewHttpClient(tls_client.NewLogger(), options...)
	return client
}""", code)


with open("main.go", "w") as f:
    f.write(code)
