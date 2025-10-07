import os
from typing import Any, Literal
from dotenv import load_dotenv
from botocore.config import Config as BotoConfig
from pydantic import BaseModel, Field


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

    def model_post_init(self, __context: Any):
        load_dotenv()

        self.aws_region = get('AWS_REGION', 'us-east-1')
        self.aws_access_key_id = get_or_throw('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = get_or_throw('AWS_SECRET_ACCESS_KEY')
        self.table_name = get('TABLE_NAME', 'scrape-state')
        self.queue_url = get_or_throw('WEB_SCRAPER_QUEUE_URL')
        self.bucket_name = get('WEB_SCRAPER_RESULTS_BUCKET', 'scrape-results')
        self.ddb_page_limit = int(get('DDB_PAGE_LIMIT', '1000'))
        self.visibility_timeout = int(get('WEB_SCRAPER_QUEUE_MESSAGE_VISIBILITY_SECS', '180'))
        self.use_local_storage = get('USE_LOCAL_STORAGE', 'false').lower() == 'true'
        self.local_storage_path = get('LOCAL_STORAGE_PATH', './storage')

    def proxy_config(self, proxy_type: Literal['datacenter', 'residential', 'web-unlocker']) -> dict[str, str]:
        proxy_url = get_or_throw(f'PROXY_URL')
        datacenter_password = get_or_throw(f'PROXY_DATACENTER_PASSWORD')
        residential_password = get_or_throw(f'PROXY_RESIDENTIAL_PASSWORD')
        web_unlocker_password = get_or_throw(f'PROXY_WEB_UNLOCKER_PASSWORD')

        if proxy_type == 'datacenter':
            proxy_password = datacenter_password
        elif proxy_type == 'residential':
            proxy_password = residential_password
        elif proxy_type == 'web-unlocker':
            proxy_password = web_unlocker_password

        return {
            'proxy_url': proxy_url,
            # 'proxy_username': datacenter_username,
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