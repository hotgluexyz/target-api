"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
from typing import List

from target_hotglue.client import HotglueBatchSink, HotglueSink

from target_api.client import ApiSink


class RecordSink(ApiSink, HotglueSink):
    def preprocess_record(self, record: dict, context: dict) -> dict:
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
            self._config.get("method", "POST").upper(), request_data=record
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
            return self.config.get("batch_size", 1)

    def process_batch_record(self, record: dict, index: int) -> dict:
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
            self._config.get("method", "POST").upper(), request_data=records
        )

        self.logger.info(f"Response: {response.status_code} - {response.text}")

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()
