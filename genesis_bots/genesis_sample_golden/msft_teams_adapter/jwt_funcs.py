from datetime import timedelta, timezone, datetime
import base64
from getpass import getpass
import hashlib
import logging
import sys
import requests
import json
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

# cryptography>=3.0
# PyJWT>=2.0.0
# requests>=2.0.0
# typing-extensions ; python_version < "3.5.2"

# This class relies on the PyJWT module (https://pypi.org/project/PyJWT/).
import jwt

try:
    from typing import Text
except ImportError:
    logger.debug(
        "# Python 3.5.0 and 3.5.1 have incompatible typing modules.", exc_info=True
    )
    from typing_extensions import Text

ISSUER = "iss"
EXPIRE_TIME = "exp"
ISSUE_TIME = "iat"
SUBJECT = "sub"


# If you generated an encrypted private key, implement this method to return
# the passphrase for decrypting your private key. As an example, this function
# prompts the user for the passphrase.
def get_private_key_passphrase():
    return getpass("Passphrase for private key: ")


class JWTGenerator(object):
    """
    Creates and signs a JWT with the specified private key file, username, and account identifier. The JWTGenerator keeps the
    generated token and only regenerates the token if a specified period of time has passed.
    """

    LIFETIME = timedelta(minutes=59)  # The tokens will have a 59-minute lifetime
    RENEWAL_DELTA = timedelta(minutes=54)  # Tokens will be renewed after 54 minutes
    ALGORITHM = "RS256"  # Tokens will be generated using RSA with SHA256

    def __init__(
        self,
        account: Text,
        user: Text,
        private_key: Text,
        lifetime: timedelta = LIFETIME,
        renewal_delay: timedelta = RENEWAL_DELTA,
    ):
        """
        __init__ creates an object that generates JWTs for the specified user, account identifier, and private key.
        :param account: Your Snowflake account identifier.
        :param user: The Snowflake username.
        :param private_key: Private key string in PEM format
        :param lifetime: The number of minutes (as a timedelta) during which the key will be valid.
        :param renewal_delay: The number of minutes (as a timedelta) from now after which the JWT generator should renew the JWT.
        """

        logger.info(
            """Creating JWTGenerator with arguments
            account : %s, user : %s, lifetime : %s, renewal_delay : %s""",
            account,
            user,
            lifetime,
            renewal_delay,
        )

        self.account = self.prepare_account_name_for_jwt(account)
        self.user = user.upper()
        self.qualified_username = self.account + "." + self.user

        self.lifetime = lifetime
        self.renewal_delay = renewal_delay
        self.renew_time = datetime.now(timezone.utc)
        self.token = None

        # Load the private key from the string
        try:
            # Try to access the private key without a passphrase
            self.private_key = load_pem_private_key(
                private_key.encode(), None, default_backend()
            )
        except TypeError:
            # If that fails, provide the passphrase returned from get_private_key_passphrase()
            self.private_key = load_pem_private_key(
                private_key.encode(),
                get_private_key_passphrase().encode(),
                default_backend(),
            )

    def prepare_account_name_for_jwt(self, raw_account: Text) -> Text:
        """
        Prepare the account identifier for use in the JWT.
        For the JWT, the account identifier must not include the subdomain or any region or cloud provider information.
        :param raw_account: The specified account identifier.
        :return: The account identifier in a form that can be used to generate the JWT.
        """
        account = raw_account
        if not ".global" in account:
            # Handle the general case.
            idx = account.find(".")
            if idx > 0:
                account = account[0:idx]
        else:
            # Handle the replication case.
            idx = account.find("-")
            if idx > 0:
                account = account[0:idx]
        # Use uppercase for the account identifier.
        return account.upper()

    def get_token(self) -> Text:
        """
        Generates a new JWT. If a JWT has already been generated earlier, return the previously generated token unless the
        specified renewal time has passed.
        :return: the new token
        """
        now = datetime.now(timezone.utc)  # Fetch the current time

        # If the token has expired or doesn't exist, regenerate the token.
        if self.token is None or self.renew_time <= now:
            logger.info(
                "Generating a new token because the present time (%s) is later than the renewal time (%s)",
                now,
                self.renew_time,
            )
            # Calculate the next time we need to renew the token.
            self.renew_time = now + self.renewal_delay

            # Prepare the fields for the payload.
            # Generate the public key fingerprint for the issuer in the payload.
            public_key_fp = self.calculate_public_key_fingerprint(self.private_key)

            # Create our payload
            payload = {
                # Set the issuer to the fully qualified username concatenated with the public key fingerprint.
                ISSUER: self.qualified_username + "." + public_key_fp,
                # Set the subject to the fully qualified username.
                SUBJECT: self.qualified_username,
                # Set the issue time to now.
                ISSUE_TIME: now,
                # Set the expiration time, based on the lifetime specified for this object.
                EXPIRE_TIME: now + self.lifetime,
            }

            # Regenerate the actual token
            token = jwt.encode(
                payload, key=self.private_key, algorithm=JWTGenerator.ALGORITHM
            )
            # If you are using a version of PyJWT prior to 2.0, jwt.encode returns a byte string instead of a string.
            # If the token is a byte string, convert it to a string.
            if isinstance(token, bytes):
                token = token.decode("utf-8")
            self.token = token
            logger.info(
                "Generated a JWT with the following payload: %s",
                jwt.decode(
                    self.token,
                    key=self.private_key.public_key(),
                    algorithms=[JWTGenerator.ALGORITHM],
                ),
            )

        return self.token

    def calculate_public_key_fingerprint(self, private_key: Text) -> Text:
        """
        Given a private key in PEM format, return the public key fingerprint.
        :param private_key: private key string
        :return: public key fingerprint
        """
        # Get the raw bytes of public key.
        public_key_raw = private_key.public_key().public_bytes(
            Encoding.DER, PublicFormat.SubjectPublicKeyInfo
        )

        # Get the sha256 hash of the raw bytes.
        sha256hash = hashlib.sha256()
        sha256hash.update(public_key_raw)

        # Base64-encode the value and prepend the prefix 'SHA256:'.
        public_key_fp = "SHA256:" + base64.b64encode(sha256hash.digest()).decode(
            "utf-8"
        )
        logger.info("Public key fingerprint is %s", public_key_fp)

        return public_key_fp


def _get_token(args):
    """
    Generate a JWT token using the JWTGenerator class
    
    Args:
        args: An object containing account, user, private_key, lifetime, and renewal_delay attributes
        
    Returns:
        The generated JWT token
    """
    token = JWTGenerator(
        args.account,
        args.user,
        args.private_key,
        timedelta(minutes=args.lifetime),
        timedelta(minutes=args.renewal_delay),
    ).get_token()
    logger.info("Key Pair JWT: %s", token)  
    return token


def token_exchange(token, role, endpoint, snowflake_account_url, snowflake_account):
    """
    Exchange a JWT token for a Snowflake session token
    
    Args:
        token: The JWT token to exchange
        role: The Snowflake role to assume
        endpoint: The Snowflake endpoint
        snowflake_account_url: Optional custom Snowflake account URL
        snowflake_account: The Snowflake account identifier
        
    Returns:
        The Snowflake session token
    """
    scope_role = f"session:role:{role}" if role is not None else None
    scope = f"{scope_role} {endpoint}" if scope_role is not None else endpoint
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "scope": scope,
        "assertion": token,
    }
    logger.info("Request data: %s", data)
    url = f"https://{snowflake_account}.snowflakecomputing.com/oauth/token"
    if snowflake_account_url:
        url = f"{snowflake_account_url}/oauth/token"
    logger.info("OAuth URL: %s", url)

    response = requests.post(url, data=data)
    logger.info("Response status code: %s", response.status_code)
    logger.info("Response headers: %s", response.headers)
    logger.info("Response body: %s", response.text)

    if response.status_code != 200:
        error_msg = "Failed to get Snowflake token. Status: %s, Response: %s" % (response.status_code, response.text)
        logger.error(error_msg)
        raise Exception(error_msg)

    return response.text


def connect_to_spcs(token, url):
    """
    Test connection to Snowflake SPCS with the provided token
    
    Args:
        token: The Snowflake session token
        url: The SPCS URL
        
    Returns:
        The response from the SPCS echo endpoint
    """
    # Create a request to the ingress endpoint with authz.
    headers = {"Authorization": f'Snowflake Token="{token}"'}
    data = {"data": [[0, "test_value"]]}  # Row index 0 with a test value
    response = requests.post(f"{url}/echo", headers=headers, json=data)
    logger.info("return code %s", response.status_code)  
    logger.info(response.text)
    return response