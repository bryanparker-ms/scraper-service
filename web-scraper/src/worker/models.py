from typing import Any, Optional

from pydantic import BaseModel, Field


class ScrapeResult(BaseModel):
    html: str
    screenshot: Optional[str] = None
    data: dict[str, Any] = Field(default_factory=dict)
