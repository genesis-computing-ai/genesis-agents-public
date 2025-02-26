# Genesis Bots - Overview

This repository contains the public open-source code for the `genesis-bots` system by Genesis Computing.
For more information, visit [Genesis Computing Documentation](https://docs.genesiscomputing.com/docs/).

The rest of this guide assumes you are familiar with the basic concepts of GenBots and wish to see the Genesis system in action by setting up and testing the system yourself.

## System Components

### Genesis Server
At its core, the Genesis system requires a running server. The server is responsible for:
- Managing the lifecycle, state, and health of the GenBots.
- Servicing calls from multiple client applications to interact with the bots, manage configuration, and set up integration with external systems (e.g., database connections).
- Continuously monitoring your data sources to keep its internal data model and semantic layers up to date.
- Driving independent tasks performed by GenBots.

### Genesis UI (Streamlit)
This repository contains a Streamlit UI application that can be used to manage the configuration of the system as well as chat directly with the GenBots configured on the system.

### GenesisAPI
Users can leverage the Genesis system to build custom agentic AI Data workflows. Genesis offers a Python API that wraps REST endpoints exposed by the server. The API allows users to interact with GenBots, create new GenBots, use custom client-side tools, push/pull content, etc.

The repository includes several scripts that demonstrate the power of the API.

## Installation

You have several options for installing and getting up and running with the system, depending on the level of visibility and 'depth' that you are interested in:

**Setup (A) - Developer mode**: Clone the repo and run the Genesis server and other applications directly from the source code.

**Setup (B) - Package mode**: `pip`-install the latest Genesis package from the Python Package Index and interact with the system through the Streamlit App, Slack, or Teams. You will only need to clone this repository if you want to run the example GenesisAPI scripts against that server or to peek into the source code.

**Setup (C) - Snowflake Native App**: Run the Genesis system as a Snowflake Native app on your own Snowflake account. The Genesis server, along with the Streamlit App, will also be running natively and securely inside your own Snowflake account. This setup is recommended for production environments. You will only need to clone this repository if you want to run the example GenesisAPI scripts against that server or to peek into the source code.

### Prerequisites

If you intend to run the server or any of the GenesisAPI examples yourself, you will need Python 3.10 and up installed.
To verify the Python version that is installed on your system, you can run the following command in your terminal or command prompt:

```sh
python3 --version
```

### Setup (A) - Developer mode
1. Clone the repo into a working directory
   ```sh
   git clone https://github.com/genesis-bots/genesis-bots.git
   cd genesis-bots
   ```
2. Set up and activate your virtual environment

   On Linux and macOS:
   ```sh
   python -m venv venv-gbots && source venv-gbots/bin/activate
   ```
   On Windows:
   ```sh
   python -m venv venv-gbots && venv-gbots\Scripts\activate
   ```

3. Install required packages
   ```sh
   pip install -r requirements.txt
   ```

Next steps:
1. Run the server locally (see below).
2. (Optional) Run the GenesisAPI example scripts (see below).

### Setup (B) - Package mode

1. Create a working directory for your project.
   ```sh
   mkdir genesis-bots
   cd genesis-bots
   ```

1. Set up and activate your virtual environment

   On Linux and macOS:
   ```sh
   python -m venv venv-gbots && source venv-gbots/bin/activate
   ```
   On Windows:
   ```sh
   python -m venv venv-gbots && venv-gbots\Scripts\activate
   ```

2. Install the genesis-bots package
   ```sh
   pip install genesis-bots
   ```
Next steps:
1. Run the server locally (see below).
2. (Optional) Run the GenesisAPI example scripts (see below).

### Setup (C) - Snowflake Native App
In this setup, we assume you already have the Genesis System running as a native Snowflake application (see [documentation](https://docs.genesiscomputing.com/docs/home)) and that you want to run the GenesisAPI example scripts, connecting them to the same server.

1. Follow the same steps as in **Setup (A)** to set up a working repository and virtual environment. This is required to get a local copy of the API examples.
2. Make sure you can connect to your Snowflake account programmatically using JWT tokens. See the [documentation](https://docs.genesiscomputing.com/docs/home) for more details.
3. Run the GenesisAPI example scripts, pointing them at the server running in your Snowflake account. See below for more details.

## Running the Genesis Server Locally
As explained above, all the Genesis applications (Streamlit UI, Slack/Teams integration, etc.) need to connect to a running Genesis server.
In production environments, the Genesis server is hosted and managed in a secured environment. For example, as a Snowflake Native app inside your own Snowflake account.

This section describes how you can run the Genesis server locally on your own machine or a machine inside your accessible network.

### Assumptions:
* You have access to a machine where you can run the server.
* You have followed the above steps for setting up a working directory and virtual environment for the Genesis system (Setup (A) or Setup (B)).

### Server State and Demo Data
The server requires a working directory to manage its state and keep track of the bots, projects, integrations, etc. The genesis-bots package/repo also includes a few sample datasets for demos and testing.
This working directory defaults to your current working directory (CWD).

When you run your server on a local machine for the first time, it will:
1. Create its internal 'state' in a local SQLite database file called `genesis.db`.
2. Look for the example databases and other resources in a `genesis_sample_data` subdirectory.
3. Create a `runtime` subdirectory for storing internal resources such as files, logs, etc.

To set up this working directory for the first time, run the following command:
```sh
genesis setup
```
Note: if you are running the source code directly from the cloned repo (Setup (A)), use the command `./genesis setup` instead.

If you want to clean up the state and start fresh, you can run the following commands:
```sh
genesis cleanup
genesis setup
```

### Running the Server (Locally)
Use the following command to start the server on your local machine:
```sh
genesis start
```
By default, this command will also start the Streamlit UI automatically in your default browser. If you want to suppress this behavior, you can add the `--no-launch-ui` flag.

Next steps:
1. Use the Streamlit UI to input your LLM provider key.
2. Go to the [docs](https://docs.genesiscomputing.com/docs/home) to get started and learn more.

## API Examples

You can find the API example Python scripts under the `api_examples` directory.
The sample scripts were designed to connect to a running Genesis server. The server manages the bots, their configuration, memory, projects, tools, integrations, database connections, etc. The server exposes an API that is available for Python programmers for building AI Data applications, using the Genesis services.

To understand the API through examples, we recommend running and reviewing the examples in the following order:

1. `cli_chat.py` - demonstrates the most basic usage of the API - a simple command line chatbot that connects with the existing bots (e.g., '@Eve') that are configured in the server.
2. `baseball_data_explorer.py` - demonstrates how to use the API to build a basic 'baseball stats' CLI application without writing any SQL.
3. `data_catalog_maintenance.py` - demonstrates how to build a process to automatically keep a data catalog up to date with the latest actual data in the database. This example also demonstrates how to use custom local tools (integration with a custom catalog API) along with the built-in Genesis tools, to create a powerful and flexible AI automation.

### Environment
The examples rely on the `genesis_bots.api` python package to available in your Python environment.
If you are installed your genesis-bots package in your current environment (Setup (B)), you can run the examples by executing the scripts directly (see below).
If you are running the source code directly from the cloned repo (Setup (A)), make sure your working directory (the root of the repo) is in your `PYTHONPATH`.

To check whether the `genesis_bots.api` package is available in your environment, run the following command:
```sh
python3 -c 'import genesis_bots.api'
```
If the command succeeds, you are all set.
Otherwise, add the root of the repo to your `PYTHONPATH`.
```sh
export PYTHONPATH="$PYTHONPATH:$(pwd)"
```


### Pointing the API examples to a running Genesis Server

In order to run any of the API examples, you need a running Genesis server, and you need to point the script to the server loation.

For convenience and simplicity, the example scripts all support the same command line arguments to control the server connection method through the `--server_url` argument, with additional arguments for specific connection methods. By default, without any additional arguments, the scripts will attempt to connect to a local server running on your machine on `http://localhost:8080`. Port `8080` is the default port for the local server to listen on for incoming connections.

For example, to run the `cli_chat` example script against a local server, you simply need to run:

```sh
python3 api_examples/cli_chat.py
```
Except for when your server is hosted in your Snowflake account (Setup (C)), in most cases you will be runinng the server on the local machine or another machine/port that is accessible from your machine, so either ommit the `--server_url` argument or specify an explicit host:port combination.
If you want to connect to the server running in your Snowflake account, you will need to first make sure you have access to programmatically connect to your Snowflake account with authentication that uses JWT tokens. See the [documentation](https://docs.genesiscomputing.com/docs/home) for more details.

To get more information on the command line arguments, you can use the `--help` argument:

```sh
python3 api_examples/cli_chat.py --help
```
```
usage: cli_chat.py [-h] [--server_url SERVER_URL] [--snowflake_conn_args SNOWFLAKE_CONN_ARGS] [--genesis_db GENESIS_DB]

A simple CLI chat interface to Genesis bots

options:
  -h, --help            show this help message and exit
  --server_url SERVER_URL, -u SERVER_URL
                        Server URL for GenesisAPI. Defaults to http://localhost:8080 It supports three types of URLs: 1. HTTP(s) server URL (e.g.,
                        "http://localhost:8080"), 2. "embedded" for running the Genesis BotOsServer inside the caller's process (used for testing
                        and development only). 3. Snowflake SQLAlchemy connection URL (e.g., "snowflake://user@account") that is passed to
                        SqlAlchemy's create_engine function. (see --snowflake_conn_args for additional connection arguments for Snowflake)
  --snowflake_conn_args SNOWFLAKE_CONN_ARGS, -c SNOWFLAKE_CONN_ARGS
                        Additional connection arguments for a Snowflake connection if the server URL is a Snowflake connection URL. Use the format
                        key1=value1,key2=value2,... (no quotes). To pass a private_key that is stored in a PEM file is "private_key_file", we load
                        the private key from the provided PEM file and add it to the arguments as "private_key".
   ...

```

## License

See the [LICENSE](LICENSE) file for details.

## Contact

For any questions or suggestions, feel free to reach out at support@genesiscomputing.ai.

---

Happy coding! 🚀
