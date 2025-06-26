"""CanvasCareer target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink

from target_hotglue.client import HotglueSink, HotglueBatchSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from singer_sdk.authenticators import BearerTokenAuthenticator
from target_canvas_career.auth import CanvasCareerAuthenticator
from singer_sdk.streams import GraphQLStream


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


class CanvasCareerGraphQLSink(GraphQLStream, HotglueBatchSink):
    """CanvasCareer target sink class."""

    @property
    def base_url(self):
        return self.config.get("metadata_service_tenant")
    
    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["metadata_service_token"],
        )
    
    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]
        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        # filter out None values from records that were patched
        records = [record for record in records if record != None]

        try:
            response = self.make_batch_request(records)
            result = self.handle_batch_response(response)
            for state in result.get("state_updates", list()):
                self.update_state(state)
        except Exception as e:
            self.logger.exception("Failed to process batch:")