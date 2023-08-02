"""Api target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue

from target_api.sinks import (
    ApiSink,
)


class TargetApi(TargetHotglue):
    """Sample target for Api."""

    name = "target-api"
    SINK_TYPES = [ApiSink]
    MAX_PARALLELISM = 10


if __name__ == "__main__":
    TargetApi.cli()
