"""CanvasCareer target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from singer_sdk.sinks import RecordSink

from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_hotglue.auth import Authenticator

from target_canvas_career.auth import CanvasCareerAuthenticator


class CanvasCareerSink(HotglueSink, RecordSink):
    """CanvasCareer target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    auth_state = {}

    @property
    def base_url(self) -> str:
        return f"https://{self.config.get('base_url')}/api/v1"
    
    @property
    def authenticator(self):
        oauth_url = f"https://{self.config.get('base_url')}/login/oauth2/token"
        return CanvasCareerAuthenticator(
            self._target, self.auth_state, oauth_url
        )
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        return headers
