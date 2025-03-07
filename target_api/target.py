"""Api target class."""

from __future__ import annotations

from typing import Type, Optional
import copy

from singer_sdk import Sink
from target_hotglue.target import TargetHotglue

from target_api.sinks import BatchSink, RecordSink
from singer_sdk.helpers._compat import final
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

    @final
    def drain_one(self, sink: Optional[Sink]) -> None:
        """Drain a specific sink.

        Args:
            sink: Sink to be drained.
        """
        # post empty records only if post_empty_record flag is set as True (it's False by default)
        if not self.config.get("post_empty_record", False):
            super().drain_one(sink)
        
        else:
            draining_status = sink.start_drain()
            # if there is schema but no records for a sink, post an empty record
            if not draining_status and (
                not sink.latest_state
                or not sink.latest_state.get("summary", {}).get(sink.name)
            ):
                draining_status = {"records": [{}]}
                sink.send_empty_record = True

            
            # send an empty record for batchSink
            if self.config.get("process_as_batch"):
                sink.process_batch(draining_status)
                sink.mark_drained()
            # send an empty record and update state for single record Sink
            else:
                sink.process_record({}, {})
                if not self._latest_state:
                    # If "self._latest_state" is empty, save the value of "sink.latest_state"
                    self._latest_state = sink.latest_state
                else:
                    for key in self._latest_state.keys():
                        sink_latest_state = sink.latest_state or dict()
                        self._latest_state[key].update(sink_latest_state.get(key) or dict())
                self._write_state_message(self._latest_state)

    def _process_record_message(self, message_dict: dict) -> None:
        """Process a RECORD message.

        Args:
            message_dict: TODO
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_name = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_name]:
            # new_schema = helpers._float_to_decimal(new_schema)
            raw_record = copy.copy(message_dict["record"])
            transformed_record = stream_map.transform(raw_record)
            if transformed_record is None:
                # Record was filtered out by the map transform
                continue

            sink = self.get_sink(stream_map.stream_alias, record=transformed_record)
            context = sink._get_context(transformed_record)
            if sink.include_sdc_metadata_properties:
                sink._add_sdc_metadata_to_record(
                    transformed_record, message_dict, context
                )
            else:
                sink._remove_sdc_metadata_from_record(transformed_record)

            sink._validate_and_parse(transformed_record)

            sink.tally_record_read()
            transformed_record = sink.preprocess_record(transformed_record, context)
            sink.process_record(transformed_record, context)
            sink._after_process_record(context)

            if not self._latest_state:
                # If "self._latest_state" is empty, save the value of "sink.latest_state"
                self._latest_state = sink.latest_state
            else:
                # If "self._latest_state" is not empty, update all its fields with the
                # fields from "sink.latest_state" (if they exist)
                for key in self._latest_state.keys():
                    sink_latest_state = sink.latest_state or dict()
                    self._latest_state[key].update(sink_latest_state.get(key) or dict())

if __name__ == "__main__":
    TargetApi.cli()
