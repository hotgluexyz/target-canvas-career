"""CanvasCareer target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue
from typing import List, Optional, Union, Type
from pathlib import PurePath
from singer_sdk.sinks import Sink

from target_canvas_career.sinks import (
    ImportSink,
    MetadataSink
)


class TargetCanvasCareer(TargetHotglue):
    """Sample target for CanvasCareer."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

    name = "target-canvas-career"

    SINK_TYPES = [ImportSink, MetadataSink]
    MAX_PARALLELISM = 1
    user_uuids = {}

    config_jsonschema = th.PropertiesList(
        th.Property("base_url",th.StringType,required=True),
        th.Property("client_id",th.StringType,required=True),
        th.Property("client_secret",th.StringType,required=True),
        th.Property("account_id",th.StringType,required=False, default="self"),
        th.Property("metadata_service_tenant",th.StringType,required=True),
        th.Property("metadata_service_token",th.StringType,required=True),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Get sink for a stream."""
        for sink_class in self.SINK_TYPES:
            # Search for streams with multiple names
            if stream_name.lower() == sink_class.name:
                return sink_class
        return ImportSink

if __name__ == "__main__":
    TargetCanvasCareer.cli()
