from mitmproxy import http

def request(flow: http.HTTPFlow) -> None:
    # Filter only requests going to port 3978
    if flow.request.port == 3978:
        print(f"🔍 Intercepted JSON Request to {flow.request.host}:{flow.request.port}")
        print(flow.request.content.decode("utf-8"))  # Print JSON body

def response(flow: http.HTTPFlow) -> None:
    # Filter only responses from port 3978
    if flow.request.port == 3978:
        print(f"📥 Intercepted JSON Response from {flow.request.host}:{flow.request.port}")
        print(flow.response.content.decode("utf-8"))