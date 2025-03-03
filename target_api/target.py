"""Api target class."""

from __future__ import annotations

from typing import Type, Optional

from singer_sdk import Sink
from target_hotglue.target import TargetHotglue

from target_api.sinks import BatchSink, RecordSink
from singer_sdk.helpers._compat import final


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

    @final
    def drain_one(self, sink: Optional[Sink]) -> None:
        """Drain a specific sink.

        Args:
            sink: Sink to be drained.
        """

        draining_status = sink.start_drain()
        # if there is schema but no records for a sink, post an empty record
        if not draining_status and (
            not sink.latest_state
            or not sink.latest_state.get("summary", {}).get(sink.name)
        ):
            draining_status = {"records": [{}]}
            sink.send_empty_record = True
        elif not sink or sink.current_size == 0:
            return

        # send an empty record for batchSink
        if self.config.get("process_as_batch"):
            sink.process_batch(draining_status)
            sink.mark_drained()
        # send an empty record and update state for single record Sink
        else:
            sink.process_record({"id": ""}, {})
            if not self._latest_state:
                # If "self._latest_state" is empty, save the value of "sink.latest_state"
                self._latest_state = sink.latest_state
            else:
                for key in self._latest_state.keys():
                    sink_latest_state = sink.latest_state or dict()
                    self._latest_state[key].update(sink_latest_state.get(key) or dict())
            self._write_state_message(self._latest_state)


if __name__ == "__main__":
    TargetApi.cli()
