
# **Genesis API**

**Description:**
A programmatic interface to the Genesis AI platform.
---

## **Installation**

Install the package via pip:

```bash
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ genesis_api_public
```

---

## **Usage**

Here’s an example of how to use the package:

```python
from genesis_api_public.genesis_api import GenesisAPI
from genesis_api_public.snowflake_remote_server import GenesisSnowflakeServer


client = GenesisAPI(server_type=GenesisSnowflakeServer, scope="GENESIS_BOTS_ALPHA")
bots = client.get_all_bots()
print(bots)

request = client.submit_message("Janice", "hello. answer in spanish")
response = client.get_response("Janice", request.request_id, timeout_seconds=10)
print(response)
request = client.submit_message("Janice", "what is the capital of spain?", thread_id=request.thread_id)
response = client.get_response("Janice", request["request_id"], timeout_seconds=10)
print(response)


client.shutdown()
```

---

## **License**

This project is licensed under the **Server Side Public License (SSPL)**.

The full text of the license is available in the [LICENSE](LICENSE) file.

Please note that the SSPL is **not an OSI-approved open-source license**. Use of this software is subject to additional conditions, primarily regarding its use in providing a publicly available cloud service.

For more details, visit:
[MongoDB SSPL License Explanation](https://www.mongodb.com/licensing/server-side-public-license)

