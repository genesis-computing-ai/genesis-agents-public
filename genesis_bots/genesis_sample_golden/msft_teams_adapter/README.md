Python utility to relay messages to and from an Azure Bot (for MS Teams) to a Genesis instance running in SPCS or any other host on www

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
     zip -r bot.zip app.py bot.py requirements.txt .env <your_key_file_path>
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
