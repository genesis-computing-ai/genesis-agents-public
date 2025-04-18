# Azure Container Registry and Web App Deployment Documentation

This document outlines the steps required to allow Genesis to connect to MS Teams.  Make sure you have set up an Azure account and have the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) installed as well as a MS365 Developer account.  You will also need to have Docker installed on your local machine.

## 1. Open a Terminal window and log in to the Azure command line interface (See Azure docs for installation):

az login

## 2. Create Azure Container Registry (ACR)

An Azure Container Registry must be created to store Docker images.

```bash
az acr create --resource-group GenesisBotAzureProxy_group --name genesisbotregistry --sku Basic
```

## 3. Enable Admin Access for ACR

Admin access must be enabled to authenticate with the registry using username and password.

```bash
az acr update --name genesisbotregistry --admin-enabled true
```

## 4. Get ACR Credentials

Retrieve the credentials to use for Docker login.

```bash
az acr credential show --name genesisbotregistry
```

## 5. Docker Login to ACR

Use the credentials to authenticate Docker with ACR.

```bash
docker login genesisbotregistry.azurecr.io -u genesisbotregistry -p <password>
```

## 6. Build Docker Image for ARM64 (Local Development with Mac Silicon Only)

Build a Docker image for ARM64 architecture (Apple Silicon).

```bash
docker buildx build --platform linux/arm64/v8 -t genesisbotregistry.azurecr.io/genesisbot:arm64 .
```

### Run the Docker image locally and make sure it is running correctly:
```bash
docker run -d -p 8000:8000 \
  -e PORT=8000 \
  -e PYTHONUNBUFFERED=1 \
  --name genesis-bot-test \
  genesisbotregistry.azurecr.io/genesisbot:arm64
```

### Confirm that the docker image is running.  you should see a container named genesis-bot-test
```bash
docker ps
```

### Now check the logs to confirm that the container is running properly.  You should see an intro message from a bot:
```bash
docker logs genesis-bot-test
```

## 7. Build Docker Image for Azure deployment - AMD64
```bash
docker buildx build -t genesisbotregistry.azurecr.io/genesisbot:amd64 .
```

## 8. Push Docker Image to ACR

Push the AMD64 image to the container registry.

```bash
docker push genesisbotregistry.azurecr.io/genesisbot:amd64
```

## 9. List Available App Service Plans

List the available app service plans to create a new web app.

```bash
az appservice plan list --resource-group GenesisBotAzureProxy_group
```

## 10. Create Web App for Containers

Create a web app configured to use the container image.

```bash
az webapp create --resource-group GenesisBotAzureProxy_group --plan GenesisBotProxyPlan --name GenesisBotProxy --deployment-container-image-name genesisbotregistry.azurecr.io/genesisbot:amd64
```

## 11. Configure Container Registry Settings for Web App

Set the credentials for the web app to access the container registry.

```bash
az webapp config container set --name GenesisBotProxy --resource-group GenesisBotAzureProxy_group --docker-registry-server-url https://genesisbotregistry.azurecr.io --docker-registry-server-user genesisbotregistry --docker-registry-server-password "<registry_password>" --docker-custom-image-name genesisbotregistry.azurecr.io/genesisbot:amd64
```

## 12. Configure Environment Variables

### Method #1:

Duplicate the file env.template and re-name it .env.  Confirm that the file .env and private_key.pem are not committed to the repository by including them in .gitignore.  Fill in the required values for the environment variables in the .env file.

### Method #2: Use the Azure CLI
Set the environment variables required by the application.

```bash
az webapp config appsettings set --name GenesisBotProxy --resource-group GenesisBotAzureProxy_group --settings PORT=8000 WEBSITES_PORT=8000 APP_ID="<your_app_id_here" APP_PASSWORD="<your_app_password>" PYTHONUNBUFFERED=1
```

Note:  The App Id can be retrieved from the Azure UI (portal.azure.com). You can't get it from a CLI command.  Navigate to your Azure Bot.  In the left side panel, click Settings, then Configuration.  The App ID is in the box labeled Microsoft App ID.  Then click the 'Manage Password' link and create a Client secret. This is the App Password.

## 13. TODO - Create an public and private pem key and set up on Snowflake.  Then add the private key to a file named private_key.pem.  The tedt should begin with '-----BEGIN PRIVATE KEY-----' and end with '-----END PRIVATE KEY-----'.  Make sure the file is not committed to the repository by adding to .gitignore.

ALTER USER <snwflake_user_NAME> SET RSA_PUBLIC_KEY = '<pem_public_key_here>';

## 14. Restart Web App

Restart the web app to apply changes.

```bash
az webapp restart --name GenesisBotProxy --resource-group GenesisBotAzureProxy_group
```

## 15. Verify Health Endpoint (Success)

Verify that the health endpoint was responding successfully.

```bash
curl -v https://genesisbotproxy3.azurewebsites.net/health
```

## 16. Check Web App Logs

Download and examine logs.

```bash
az webapp log download --name GenesisBotProxy2 --resource-group GenesisBotAzureProxy_group
unzip -l webapp_logs.zip
unzip -p webapp_logs.zip "LogFiles/2025_03_26_10-30-3-152_docker.log" | tail -n 100
unzip -p webapp_logs.zip "LogFiles/2025_03_26_10-30-3-152_default_docker.log" | tail -n 100
```

## 17. Test app in web chat

In the Azure UI, navigate to Azure Bots, select your bot, then click on 'Test in Web Chat'.  Make sure you can converse with your bot as expected.

## 18. Add Teams App Package

This will pull stock icons from Azure and create the manifest.json file.

```bash
curl -s https://raw.githubusercontent.com/microsoft/BotBuilder-Samples/main/samples/javascript_nodejs/02.echo-bot/deploymentTemplates/1.png -o /Users/jeff/Documents/WWW2020/GENESIS/genesis_bots/genesis_sample_golden/msft_teams_adapter/teams_app_package/outline.png
```

## 19. Create a zip file of your Teams app package

```bash
cd /Users/jeff/Documents/WWW2020/GENESIS/genesis_bots/genesis_sample_golden/msft_teams_adapter/teams_app_package && zip -r ../genesis_bot_teams_app.zip *
```

## 20 Add the Teams Channel to Your Bot
In your Azure Bot resource, go to "Channels" in the left menu
Click on the Microsoft Teams icon
Accept the terms and click "Save"

## 21. Deploy Your Teams App Package
Open Microsoft Teams desktop client
Go to the Apps section
Click "Upload a custom app" (you might need to click "..." to see this option)
Select the genesis_bot_teams_app.zip file we just created
Follow the prompts to add the bot to your team or personal scope
Click 'Test in Teams App'

## 22. Submit to Teams App Store (for organization-wide deployment)
Go to the Microsoft Teams Admin Center
Navigate to "Teams apps" > "Manage apps"
Click "Upload new app"
Upload the genesis_bot_teams_app.zip file
Once approved, users in your organization can find it in the Teams app store

## 23. Verify the Connection
After adding the bot to Teams, send a message to it
Check your bot's logs to ensure it's receiving and processing messages
The bot should respond based on your implementation

==========================================================================================

BELOW ARE THE ORIGINAL INSTRUCTIONS FOR DEPLOYING THE PROXY TO AZURE VIA ZIP FILE, NOT DOCKERFILE


Python utility to relay messages to and from an Azure Bot (for MS Teams) to a Genesis instance running in SPCS or any other host on www.

• A bot is created on Azure (portal.azure.com) for each Genesis bot that you want to use.  It will have ita own instance of the msft teams adapter.  Type 'azure bot' in the search bar and select 'Azure Bot' which will be under the 'Marketplace' heading.
• The bot is specified in the BOT_ID environment variable.

1.  Sign up for an Azure account.  The free tier is sufficient to set up a bot and adapter
2.  Sign up for an M365 developer account.  This is required to deploy a bot to Teams.
3.  Make sure the Azure command line interface (CLI) is installed on your local device.  Run 'az --version' on your command line to verify
4.  Log in using 'az login' and follow the prompts.
5.  Make sure your Azure account is connected to at least one subscription.  Set the subscription id using az cli or UI.  For az cli:
    az account set --subscription "<subscription_name_or_id>"
6.  Create a resource group on Azure using az cli or UI:
    az group create --name <your_resource_group_name> --location <your_location>
7.  To get a list of regions:
    az account list-locations -o table
8.  Verify resource group creation:
    az group show --name <your_resource_group_name>
9.  Create a bot on Azure:
    In Azure UI, search for 'Azure Bot'.  Choose Marketplace -> Azure Bot
    Add a Bot Handle
    Select your subscription
    Select your resource group
    Select your pricing tier.  Free tier is ok
    Choose 'Multi-tenant' as Type of App
    Creation type: Create new Microsoft App ID
10.  Download folder msft_teams_adapter from the Genesis Samples Golden folder
11.  Add a private key file with your own RSA private key
12.  Update the required values in the envTemplate file with your own information including the name of the private key file from the
     previous step.  Copy the file and rename to .env.  This file is excluded in .gitignore.
13.  Zip the files and name the archive bot.zip
     zip -r bot.zip app.py bot.py startup.sh requirements.txt runtime.txt web.config .env <your_key_file_path>
14.  Use the following Azure CLI commands to setup and deploy an Azure Web App:
    az appservice plan create \
      --resource-group "<your_resource_group_name>" \
      --name "<your_plan_name>" \
      --sku F1 \
      --is-linux \
      --location "canadacentral"

    az webapp create \
      --resource-group "<your_resource_group_name>" \
      --plan "<your_plan_name>" \
      --name "<your_app_name>" \
      --runtime "PYTHON:3.10"

    az webapp log config \
      --resource-group "<your_resource_group_name>" \
      --name "<your_app_name>" \
      --web-server-logging filesystem \
      --docker-container-logging filesystem \
      --detailed-error-messages true \
      --failed-request-tracing true

    az webapp config appsettings set \
      --resource-group "<your_resource_group_name>" \
      --name "<your_app_name>" \
      --settings \
      SCM_DO_BUILD_DURING_DEPLOYMENT=true \
      ENABLE_ORYX_BUILD=true \
      PYTHON_ENABLE_VENV_CREATION=true \
      WEBSITE_HTTPLOGGING_RETENTION_DAYS=7 \
      WEBSITES_PORT=8000 \
      HTTP_PLATFORM_PORT=8000

    az webapp config set --resource-group <your_resource_group_name>  --name <your_app_name> --startup-file "python app.py"

    rm -f bot.zip
    zip -r bot.zip app.py bot.py requirements.txt .env <private_key_file>

    az webapp deploy \
      --resource-group "<your_resource_group_name>" \
      --name "<your_app_name>" \
      --src-path bot.zip \
      --type zip

    # this may time out, if so do this:

    az webapp config set --resource-group <your_resource_group_name> --name <your_app_name> --startup-file "python app.py"

    az webapp deploy \
        --resource-group "<your_resource_group_name>" \
        --name "<your_app_name>" \
        --src-path bot.zip \
        --type zip

15. You can 'tail' the logs from the deployed container on the command line.  You can also view the logs on the Azure UI by navigating to Web Apps -> Log Stream:
    az webapp log tail --name "<your_app_name>" --resource-group "<your_resource_group_name>"
