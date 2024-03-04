"""
This block can be used to store credentials
to be used to authenticate to Soda Cloud APIs.
"""

from logging import Logger

from prefect.blocks.core import Block
from pydantic import Field, SecretStr, validator

from prefect_soda_cloud.soda_cloud_client import SodaCloudClient


class SodaCloudCredentials(Block):
    """
    TODO
    """

    _block_type_name = "Soda Cloud Credentials"
    _logo_url = "https://CHANGEME.todo"  # noqa
    _documentation_url = "https://AlessandroLollo.github.io/prefect-soda-cloud/blocks/#prefect-soda-cloud.auth_config.SodaCloudCredentials"  # noqa

    user_or_api_key_id: str = Field(
        name="Username or API Key ID",
        default=None,
        description="Soda Cloud username or API Key ID",
    )
    pwd_or_api_key_secret: SecretStr = Field(
        name="Password or API Key Secret",
        description="Soda Cloud password or API Key Secret",
    )


class SodaCloudAuthConfig(Block):
    """
    TODO
    """

    _block_type_name = "Soda Cloud Auth Config"
    _logo_url = "https://CHANGEME.todo"  # noqa
    _documentation_url = "https://AlessandroLollo.github.io/prefect-soda-cloud/blocks/#prefect-soda-cloud.auth_config.SodaCloudAuthConfig"  # noqa

    api_base_url: str = Field(
        name="Soda Cloud Base API URL",
        default="https://cloud.soda.io",
        description="Soda Cloud Base API URL",
    )
    creds: SodaCloudCredentials
    wait_secs_between_api_calls: int = Field(
        name="Wait time between API calls",
        default=10,
        description="Wait time in seconds between API calls. Must be >=0",
    )

    @validator("wait_secs_between_api_calls")
    def _validate_wait_secs_between_api_calls(cls, value: int) -> int:
        """
        TODO
        """
        if value < 0:
            raise ValueError("wait_secs_between_api_calls must be >= 0")
        return value

    def get_client(self, logger: Logger) -> SodaCloudClient:
        """
        TODO
        """
        return SodaCloudClient(
            api_base_url=self.api_base_url,
            username=self.creds.user_or_api_key_id,
            password=self.creds.pwd_or_api_key_secret.get_secret_value(),
            logger=logger,
            wait_secs_between_api_calls=self.wait_secs_between_api_calls,
        )
