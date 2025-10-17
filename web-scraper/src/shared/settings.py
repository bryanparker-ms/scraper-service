import os
from typing import Any, Literal, TypedDict
from dotenv import load_dotenv
from botocore.config import Config as BotoConfig
from pydantic import BaseModel, Field

class ProxyConfig(TypedDict):
    proxy_url: str
    proxy_username: str
    proxy_password: str

class Settings(BaseModel):
    aws_region: str = Field(default='us-east-1')
    aws_access_key_id: str = Field(default='')
    aws_secret_access_key: str = Field(default='')
    table_name: str = Field(default='scrape-state')
    queue_url: str = Field(default='')
    bucket_name: str = Field(default='scrape-results')
    ddb_page_limit: int = Field(default=1000)
    visibility_timeout: int = Field(default=60 * 3) # 3 minutes
    use_local_storage: bool = Field(default=False)
    local_storage_path: str = Field(default='./storage')
    api_key: str = Field(default='')

    def model_post_init(self, __context: Any):
        load_dotenv()

        self.aws_region = get('AWS_REGION', 'us-east-1')
        self.aws_access_key_id = get('AWS_ACCESS_KEY_ID', '')
        self.aws_secret_access_key = get('AWS_SECRET_ACCESS_KEY', '')
        self.table_name = get('TABLE_NAME', 'scrape-state')
        self.queue_url = get_or_throw('WEB_SCRAPER_QUEUE_URL')
        self.bucket_name = get('WEB_SCRAPER_RESULTS_BUCKET', 'scrape-results')
        self.ddb_page_limit = int(get('DDB_PAGE_LIMIT', '1000'))
        self.visibility_timeout = int(get('WEB_SCRAPER_QUEUE_MESSAGE_VISIBILITY_SECS', '180'))
        self.use_local_storage = get('USE_LOCAL_STORAGE', 'false').lower() == 'true'
        self.local_storage_path = get('LOCAL_STORAGE_PATH', './storage')
        self.api_key = get('API_KEY', '')

    def proxy_config(self, proxy_type: Literal['datacenter', 'residential', 'web-unlocker']) -> ProxyConfig:
        """
        Get proxy configuration for the specified proxy type.

        Args:
            proxy_type: Type of proxy (datacenter, residential, web-unlocker)

        Returns:
            Dict with proxy_url (host:port), proxy_username, proxy_password

        Raises:
            ValueError: If proxy credentials are not configured in environment
        """
        # Check that base proxy URL is configured
        try:
            proxy_url = get_or_throw('PROXY_URL')
        except ValueError as e:
            raise ValueError(f'Proxy requested but PROXY_URL not configured in environment: {e}')

        # Map proxy type to environment variable names
        env_mappings = {
            'datacenter': ('PROXY_DATACENTER_USER', 'PROXY_DATACENTER_PASSWORD'),
            'residential': ('PROXY_RESIDENTIAL_USER', 'PROXY_RESIDENTIAL_PASSWORD'),
            'web-unlocker': ('PROXY_WEB_UNLOCKER_USER', 'PROXY_WEB_UNLOCKER_PASSWORD'),
        }

        if proxy_type not in env_mappings:
            raise ValueError(f'Unknown proxy type: {proxy_type}')

        user_key, pass_key = env_mappings[proxy_type]

        # Get credentials for the specific proxy type
        try:
            proxy_username = get_or_throw(user_key)
            proxy_password = get_or_throw(pass_key)
        except ValueError as e:
            raise ValueError(f'Proxy type "{proxy_type}" requested but credentials not configured: {e}')

        return {
            'proxy_url': proxy_url,
            'proxy_username': proxy_username,
            'proxy_password': proxy_password,
        }

    @property
    def boto_config(self) -> BotoConfig:
        return BotoConfig(
            region_name=self.aws_region,
            retries={'max_attempts': 5, 'mode': 'standard'},
        )


def get(key: str, default: str = '') -> str:
    return os.getenv(key, default)


def get_or_throw(key: str) -> str:
    value = get(key)
    if not value:
        raise ValueError(f'{key} is not set')
    return value