"""Api target sink class, which handles writing streams."""

from __future__ import annotations
from pydantic import BaseModel

from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueSink


class ApiSink(HotglueSink):
    name = "api"

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
            record["metadata"] = self.config.get("metadata")
        return record

    def upsert_record(self, record: dict, context: dict):
        response = self.request_api(self._config.get("method", "POST").upper(), request_data=record)
        id = response.json().get("id")
        return id, response.ok, dict()
