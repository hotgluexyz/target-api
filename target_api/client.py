from __future__ import annotations

import os

from pydantic import BaseModel
from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueBaseSink


class ApiSink(HotglueBaseSink):
    @property
    def name(self):
        return self.stream_name

    @property
    def authenticator(self):
        return (
            ApiAuthenticator(
                self._target,
                header_name=self._config.get("api_key_header") or "x-api-key",
            )
            if self._config.get("auth", False)
            else None
        )

    @property
    def base_url(self) -> str:
        tenant_id = os.environ.get("TENANT")
        flow_id = os.environ.get("FLOW")

        return self._config["url"].format(
            stream=self.stream_name,
            tenant=tenant_id,
            tenant_id=tenant_id,
            flow=flow_id,
            flow_id=flow_id,
        )

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None
