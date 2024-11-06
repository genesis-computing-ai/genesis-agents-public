#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os

""" Bot Configuration """


class DefaultConfig:
    """ Bot Configuration """
    '''
    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "723e372b-4e3d-495a-bcb8-10ab10675ec6")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "TeR8Q~wTTqTlKXz2xSMUyrKXNqfuOKSFGMaEuaQo")
    APP_TYPE = os.environ.get("MicrosoftAppType", "MultiTenant")
    APP_TENANTID = os.environ.get("MicrosoftAppTenantId", "7b04cdc5-c5a1-4618-a142-bd5e98414923")
'''
    PORT= 3978
    APP_ID= os.environ.get("MicrosoftAppID", "f96bc6bd-b92c-4d9b-8258-b1fb659d6e8e")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", ".Y68Q~Ftdg8iKYp59dVTxpQ2JMcxsHyb0j0MQcNN")
    APP_TYPE = os.environ.get("MicrosoftAppType", "MultiTenant")
    APP_TENANTID = os.environ.get("MicrosoftAppTenantId", "7b04cdc5-c5a1-4618-a142-bd5e98414923")

class DefaultConfig_empty:
    """ Bot Configuration """

    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
    APP_TYPE = os.environ.get("MicrosoftAppType", "MultiTenant")
    APP_TENANTID = os.environ.get("MicrosoftAppTenantId", "")
