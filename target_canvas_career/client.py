"""CanvasCareer target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink

from target_hotglue.client import HotglueSink, HotglueBatchSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional
from target_canvas_career.auth import CanvasCareerAuthenticator
import requests
import json
import backoff
from target_hotglue.rest import RetriableAPIError, HGJSONEncoder


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
        return CanvasCareerAuthenticator(self._target, self.auth_state, oauth_url)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        return headers


class CanvasCareerGraphQLSink(HotglueBatchSink):
    """CanvasCareer target sink class."""

    @property
    def base_url(self):
        return self.config.get("metadata_service_tenant")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Authorization"] = f"Bearer {self.config.get('metadata_service_token')}"
        return headers

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]
        records = list(
            map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records))
        )

        # filter out None values from records that were patched
        records = [record for record in records if record != None]

        try:
            failed_records, response = self.make_batch_request(records)
            result = self.handle_batch_response(failed_records, response)
            for state in result.get("state_updates", list()):
                self.update_state(state)
        except Exception as e:
            self.logger.exception("Failed to process batch:")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self,
        http_method,
        endpoint,
        params={},
        request_data=None,
        headers={},
        verify=True,
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
            verify=verify,
        )
        self.validate_response(response)
        return response

    def to_graphql_input(self, value):
        if isinstance(value, dict):
            return (
                "{"
                + ", ".join(
                    f"{k}: {self.to_graphql_input(v)}" for k, v in value.items()
                )
                + "}"
            )
        elif isinstance(value, list):
            return "[" + ", ".join(self.to_graphql_input(v) for v in value) + "]"
        elif isinstance(value, str):
            return json.dumps(value)  # adds quotes and escapes properly
        elif value is None:
            return "null"
        else:
            return str(value)
