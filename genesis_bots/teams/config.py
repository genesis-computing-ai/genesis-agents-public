#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

import os

class DefaultConfig:
    """ Bot Configuration """

    PORT = 3978
    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")

    # APP_ID = os.environ.get("MicrosoftAppId", "4a16d0db-e475-49d3-881c-d6528c129f3e")
    # APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "EeT8Q~ZHDBV2Ize4Sufm-eAd6WksPs6gbMpPNads")