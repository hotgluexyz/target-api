"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
from typing import List

from target_hotglue.client import HotglueBatchSink, HotglueSink

from target_api.client import ApiSink


class RecordSink(ApiSink, HotglueSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
        if self.config.get("add_stream_key"):
            record["stream"] = self.stream_name

        if self.config.get("metadata", None):
            metadata = record.get("metadata") or {}

            try:
                metadata.update(json.loads(self.config.get("metadata")))
            except:
                metadata.update(self.config.get("metadata"))

            record["metadata"] = metadata
        return record

    def upsert_record(self, record: dict, context: dict):
        response = self.request_api(
            self._config.get("method", "POST").upper(), request_data=record, headers=self.custom_headers
        )

        self.logger.info(f"Response: {response.status_code} - {response.text}")

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()


class BatchSink(ApiSink, HotglueBatchSink):
    @property
    def max_size(self):
        if self.config.get("process_as_batch"):
            batch_size = self.config.get("batch_size", 100)
            if batch_size:
                return int(batch_size)
        return 100

    def process_batch_record(self, record: dict, index: int) -> dict:
        if self.config.get("add_stream_key"):
            record["stream"] = self.stream_name
            
        if self.config.get("metadata", None):
            metadata = record.get("metadata") or {}

            try:
                metadata.update(json.loads(self.config.get("metadata")))
            except:
                metadata.update(self.config.get("metadata"))

            record["metadata"] = metadata
        return record

    def make_batch_request(self, records: List[dict]):
        response = self.request_api(
            self._config.get("method", "POST").upper(), request_data=records, headers=self.custom_headers
        )

        self.logger.info(f"Response: {response.status_code} - {response.text}")

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()

    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        try:
            response = self.make_batch_request(records)
            result = self.handle_batch_response(response)
            for state in result.get("state_updates", list()):
                self.update_state(state)
        except Exception as e:
            self.update_state({"error": str(e)})
