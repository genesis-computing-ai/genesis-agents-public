from mitmproxy import http

def request(flow: http.HTTPFlow) -> None:
    if flow.request.port == 8081:  # Traffic redirected from 3978 → 8080
        flow.request.host = "127.0.0.1"
        flow.request.port = 3978  # Forward to the real application
        print(f"✅ Forwarding request to {flow.request.host}:{flow.request.port}")

def response(flow: http.HTTPFlow) -> None:
    print(f"📥 Response received from {flow.request.host}:{flow.request.port}")