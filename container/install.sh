#!/bin/bash

GITHUB_USER=${GITHUB_USER:-"genesis-computing-ai"}

mkdir -p $HOME/.genesis/db $HOME/bin

cat > $HOME/bin/genesis_cli <<EOF 
#!/bin/sh
if [ -t 0 ]; then
    ## Script is connected to a TTY.
    docker run --rm -it --add-host=host.docker.internal:host-gateway -e GROOT=/tmp/g -v $HOME/.genesis/cli:/tmp/g ghcr.io/$GITHUB_USER/genesis /app/api_examples/g.py \$@
else
  ## Script is not connected to a TTY.
  while read -r line; do
      echo "\$line"
  done | docker run --rm -i --add-host=host.docker.internal:host-gateway -e GROOT=/tmp/g -v $HOME/.genesis/cli:/tmp/g ghcr.io/$GITHUB_USER/genesis /app/api_examples/g.py \$@ 
fi
EOF
chmod ug+x $HOME/bin/genesis_cli
echo installed $HOME/bin/genesis_cli

cat > $HOME/bin/genesis_update <<EOF 
#!/bin/sh

IMAGE="genesis"

while getopts ":hi:" opt; do
  case "\$opt" in
    h)
      echo "usage: \$0 [-i image]" >&2
      exit 0
      ;;
    i)
      IMAGE="\$OPTARG"
      echo "using image: \$IMAGE"
      ;;
    :)
      echo "error: option -\${OPTARG} requires an argument." >&2
      exit 1
      ;;
    \?)
      echo "error: invalid option -\$OPTARG" >&2
      echo "usage: \$0 [-i image]" >&2
      exit 1
      ;;
  esac
done

shift \$((OPTIND - 1))

docker pull ghcr.io/$GITHUB_USER/\$IMAGE 
EOF
chmod ug+x $HOME/bin/genesis_update
echo installed $HOME/bin/genesis_update

if [[ -e "$HOME/.genesis/config.env" ]]; then
    echo "using exising $HOME/.genesis/config.env"
else
    cat > $HOME/.genesis/config.env <<EOF
SQLITE_DB_PATH=/app/.genesis/db/genesis.db

### To run in Snowflake mode: uncomment and set below variables:
#SNOWFLAKE_METADATA=TRUE
#SNOWFLAKE_ACCOUNT_OVERRIDE=""
#SNOWFLAKE_USER_OVERRIDE=""
#SNOWFLAKE_PASSWORD_OVERRIDE=""
#SNOWFLAKE_DATABASE_OVERRIDE="GENESIS_TEST"
#GENESIS_INTERNAL_DB_SCHEMA="XXX.YYY"
###

EOF

    if [[ -n "$OPENAI_API_KEY" ]]; then
	echo "OPENAI_API_KEY=$OPENAI_API_KEY" >> $HOME/.genesis/config.env
	echo "using provided OPENAI_API_KEY=$OPENAI_API_KEY"
    else
	echo "OPENAI_API_KEY=\"\"" >> $HOME/.genesis/config.env
	echo "OPENAI_API_KEY is not set in $HOME/.genesis/config.env"
    fi

    if [[ -n "$FLASK_HTTP_PORT" ]]; then
	echo "FLASK_HTTP_PORT=$FLASK_HTTP_PORT" >> $HOME/.genesis/config.env
	echo "using provided FLASK_HTTP_PORT=$FLASK_HTTP_PORT"
    fi

    if [[ -n "$FLASK_HTTPS_PORT" ]]; then
	echo "FLASK_HTTPS_PORT=$FLASK_HTTPS_PORT" >> $HOME/.genesis/config.env
	echo "using provided FLASK_HTTPS_PORT=$FLASK_HTTPS_PORT"
    fi

    if [[ -n "$FLASK_SERVER_HOST" ]]; then
	echo "FLASK_SERVER_HOST=$FLASK_SERVER_HOST" >> $HOME/.genesis/config.env
	echo "using provided FLASK_SERVER_HOST=$FLASK_SERVER_HOST"
    fi

    if [[ -n "$FASTAPI_PORT" ]]; then
	echo "FASTAPI_PORT=$FASTAPI_PORT" >> $HOME/.genesis/config.env
	echo "using provided FASTAPI_PORT=$FASTAPI_PORT"
    fi

    if [[ -n "$FASTAPI_HOST" ]]; then
	echo "FASTAPI_HOST=$FASTAPI_HOST" >> $HOME/.genesis/config.env
	echo "using provided FASTAPI_HOST=$FASTAPI_HOST"
    fi

    if [[ -n "$FASTAPI_BASE_URL" ]]; then
	echo "FASTAPI_BASE_URL=$FASTAPI_BASE_URL" >> $HOME/.genesis/config.env
	echo "using provided FASTAPI_BASE_URL=$FASTAPI_BASE_URL"
    fi

    if [[ -n "$SSL_CONTEXT" ]]; then
	echo "SSL_CONTEXT=$SSL_CONTEXT" >> $HOME/.genesis/config.env
	echo "using provided SSL_CONTEXT=$SSL_CONTEXT"
    fi

    if [[ -n "$UDF_BASE_URL" ]]; then
	echo "UDF_BASE_URL=$UDF_BASE_URL" >> $HOME/.genesis/config.env
	echo "using provided UDF_BASE_URL=$UDF_BASE_URL"
    fi

    if [[ -n "$BASIC_AUTH_USERNAME" ]]; then
	echo "BASIC_AUTH_USERNAME=$BASIC_AUTH_USERNAME" >> $HOME/.genesis/config.env
	echo "using provided BASIC_AUTH_USERNAME=$BASIC_AUTH_USERNAME"
    fi

    if [[ -n "$BASIC_AUTH_PASSWORD" ]]; then
	echo "BASIC_AUTH_PASSWORD=$BASIC_AUTH_PASSWORD" >> $HOME/.genesis/config.env
	echo "using provided BASIC_AUTH_PASSWORD=$BASIC_AUTH_PASSWORD"
    fi

    if [[ -n "$EXTRA_HTTP_CORS_ORIGINS" ]]; then
	echo "EXTRA_HTTP_CORS_ORIGINS=$EXTRA_HTTP_CORS_ORIGINS" >> $HOME/.genesis/config.env
	echo "using provided EXTRA_HTTP_CORS_ORIGINS=$EXTRA_HTTP_CORS_ORIGINS"
    fi

    if [[ -n "$EXTRA_HTTPS_CORS_ORIGINS" ]]; then
	echo "EXTRA_HTTPS_CORS_ORIGINS=$EXTRA_HTTPS_CORS_ORIGINS" >> $HOME/.genesis/config.env
	echo "using provided EXTRA_HTTPS_CORS_ORIGINS=$EXTRA_HTTPS_CORS_ORIGINS"
    fi
fi

cat > $HOME/bin/genesis_server <<EOF
#!/bin/sh

IMAGE="genesis"
DETACHED="-it"

while getopts ":hdi:" opt; do
  case "\$opt" in
    h)
      echo "usage: \$0 [-d] [-i image]" >&2
      exit 0
      ;;
    d)
      DETACHED="-d"
      echo "running container in detached mode"
      ;;
    i)
      IMAGE="\$OPTARG"
      echo "using image: \$IMAGE"
      ;;
    :)
      echo "error: option -\${OPTARG} requires an argument." >&2
      exit 1
      ;;
    \?)
      echo "error: invalid option -\$OPTARG" >&2
      echo "usage: \$0 [-d] [-i image]" >&2
      exit 1
      ;;
  esac
done

shift \$((OPTIND - 1))

docker run --rm \$DETACHED -p 8080:8080 -p 8501:8501 --env-file $HOME/.genesis/config.env -v $HOME/.genesis/db:/app/.genesis/db -v $HOME/.genesis/bot_git:/app/bot_git -v $HOME/.genesis/bot_storage:/app/bot_storage -v $HOME/.genesis/customer_demos:/app/customer_demos -v $HOME/.genesis/tmp:/app/tmp ghcr.io/$GITHUB_USER/\$IMAGE
EOF
chmod ug+x $HOME/bin/genesis_server
echo installed $HOME/bin/genesis_server

$HOME/bin/genesis_cli -c server_url http://host.docker.internal:8080
