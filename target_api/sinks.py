"""Api target sink class, which handles writing streams."""
from __future__ import annotations

import json
import os
from pydantic import BaseModel

from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueSink
import backoff
import requests
from singer_sdk.exceptions import RetriableAPIError
from target_hotglue.common import HGJSONEncoder


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
        tenant_id = os.environ.get("TENANT")
        flow_id = os.environ.get("FLOW")

        return self._config["url"].format(
            stream=self.stream_name,
            tenant=tenant_id,
            tenant_id=tenant_id,
            flow=flow_id,
            flow_id=flow_id
        )

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None

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
        response = self.request_api(self._config.get("method", "POST").upper(), request_data=record)

        self.logger.info(f"Response: {response.status_code} - {response.text}")

        id = None

        try:
            id = response.json().get("id")
        except Exception as e:
            self.logger.warning(f"Unable to get response's id: {e}")

        return id, response.ok, dict()
    
    @backoff.on_exception(
    backoff.expo,
    (RetriableAPIError, requests.exceptions.ReadTimeout),
    max_tries=10,
    factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data
            else None
        )

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            auth = ("PRECORO", "Sage2023!")
        )
        self.validate_response(response)
        return response
