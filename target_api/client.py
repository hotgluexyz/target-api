from __future__ import annotations

import os

from pydantic import BaseModel
from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueBaseSink
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


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
        tap = os.environ.get('TAP', None)
        connector_id = os.environ.get('CONNECTOR_ID', None)

        return self._config["url"].format(
            stream=self.stream_name,
            tenant=tenant_id,
            tenant_id=tenant_id,
            flow=flow_id,
            flow_id=flow_id,
            tap=tap,
            connector_id=connector_id
        )

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None
    
    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            error = {"status_code": response.status_code, "body":response.text}
            raise RetriableAPIError(error)
        elif 400 <= response.status_code < 500:
            try:
                msg = response.text
            except:
                msg = self.response_error_message(response)
            error = {"status_code": response.status_code, "body":msg}
            raise FatalAPIError(error)
