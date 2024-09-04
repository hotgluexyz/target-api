"""Api target class."""

from __future__ import annotations

from typing import Type

from singer_sdk import Sink
from target_hotglue.target import TargetHotglue

from target_api.sinks import BatchSink, RecordSink
from collections import OrderedDict


class TargetApi(TargetHotglue):
    """Sample target for Api."""

    name = "target-api"
    SINK_TYPES = [RecordSink, BatchSink]
    target_counter = {}

    @property
    def MAX_PARALLELISM(self):
        # If we want to process sequentially we cannot use parallelism
        # https://github.com/meltano/sdk/blob/main/singer_sdk/target_base.py#L521
        if self.config.get("enforce_order"):
            return 1

        return 10

    def __init__(
        self,
        config = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        super().__init__(config, parse_env_config, validate_config, state)

        # NOTE: We want to override this with an ordered dict to enforce order when we iterate later
        self._sinks_active = OrderedDict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        if self.config.get("process_as_batch"):
            return BatchSink
        return RecordSink


if __name__ == "__main__":
    TargetApi.cli()
