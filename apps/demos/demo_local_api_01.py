from   genesis_bots.api         import GenesisAPI, RESTGenesisServerProxy, EmbeddedGenesisServerProxy, SPCSServerProxy



# choose which server proxy mode to use
server_proxy = EmbeddedGenesisServerProxy(fast_start=True)
server_proxy = RESTGenesisServerProxy() # default to localhost

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
