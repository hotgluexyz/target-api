"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
from pydantic import BaseModel

from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueSink


class ApiSink(HotglueSink):
    @property
    def name(self):
        return self.stream_name

    @property
    def authenticator(self):
        return (
            ApiAuthenticator(
                self._target,
                header_name=self._config.get("api_key_header") or "x-api-key"
            )
            if self._config.get("auth", False)
            else None
        )

    @property
    def base_url(self) -> str:
        return self._config["url"].format(stream=self.stream_name)

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        if self.config.get("metadata", None):
            try:
                record["metadata"] = json.loads(self.config.get("metadata"))
            except:
                record["metadata"] = self.config.get("metadata")
        return record

    def upsert_record(self, record: dict, context: dict):
        response = self.request_api(self._config.get("method", "POST").upper(), request_data=record)

        self.logger.info(f"Response: {response.status_code} - {response.text}")

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()
