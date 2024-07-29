#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os

""" Bot Configuration """


class DefaultConfig:
    """ Bot Configuration """

    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "723e372b-4e3d-495a-bcb8-10ab10675ec6")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "TeR8Q~wTTqTlKXz2xSMUyrKXNqfuOKSFGMaEuaQo")
    APP_TYPE = os.environ.get("MicrosoftAppType", "MultiTenant")
    APP_TENANTID = os.environ.get("MicrosoftAppTenantId", "7b04cdc5-c5a1-4618-a142-bd5e98414923")
