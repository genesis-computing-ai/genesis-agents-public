from   genesis_bots.api         import GenesisAPI, RESTGenesisServerProxy, EmbeddedGenesisServerProxy, SPCSServerProxy



# choose which server proxy mode to use
server_proxy = EmbeddedGenesisServerProxy(fast_start=True)
server_proxy = RESTGenesisServerProxy() # default to localhost


# XXXAD: testing

ACCOUNT = 'dshrnxx-genesis-dev-consumer'
USER = 'aviv.dekel@genesiscomputing.ai'
PRIVATE_KEY_PATH = "/home/dekela/repos/genesis/.personal/secrets/rsa_key.p8"
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
with open(PRIVATE_KEY_PATH, "rb") as key:
    p_key= serialization.load_pem_private_key(key.read(), password=None, backend=default_backend()
    )
pkb = p_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8,encryption_algorithm=serialization.NoEncryption())

server_proxy = SPCSServerProxy(
    f'snowflake://{USER}@{ACCOUNT}',
    connect_args=dict(authenticator='snowflake_jwt', private_key=pkb)
)

with GenesisAPI(server_proxy=server_proxy) as client:
    print("-----------------------")
    msg = "hello"
    print(f"\n>>>> Sending '{msg}' to Janice")
    request = client.add_message("Janice", msg)
    response = client.get_response("Janice", request["request_id"])
    print(f"\n>>>> Response from Janice: {response}")

    msg = "Run a query to get the current date"
    request = client.add_message("Janice", msg)
    response = client.get_response("Janice", request["request_id"])
    print("\n>>>>", response)
