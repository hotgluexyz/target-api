"""Api target class."""

from __future__ import annotations

from typing import Type

from singer_sdk import Sink
from target_hotglue.target import TargetHotglue

from target_api.sinks import BatchSink, RecordSink


class TargetApi(TargetHotglue):
    """Sample target for Api."""

    name = "target-api"
    SINK_TYPES = [RecordSink, BatchSink]
    MAX_PARALLELISM = 10
    target_counter = {}

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        if self.config.get("process_as_batch"):
            return BatchSink
        return RecordSink


if __name__ == "__main__":
    TargetApi.cli()
