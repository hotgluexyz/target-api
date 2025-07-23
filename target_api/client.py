from __future__ import annotations

import json
import os
from sys import getsizeof

from pydantic import BaseModel
from target_hotglue.auth import ApiAuthenticator
from target_hotglue.client import HotglueBaseSink
from target_hotglue.common import HGJSONEncoder
import requests
import urllib3
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
import backoff


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
            if self._config.get("auth", False) or self._config.get("api_key_url")
            else None
        )

    @property
    def base_url(self) -> str:
        tenant_id = os.environ.get("TENANT")
        flow_id = os.environ.get("FLOW")
        tap = os.environ.get("TAP", None)
        connector_id = os.environ.get("CONNECTOR_ID", None)

        base_url = self._config["url"].format(
            stream=self.stream_name,
            tenant=tenant_id,
            tenant_id=tenant_id,
            flow=flow_id,
            flow_id=flow_id,
            tap=tap,
            connector_id=connector_id,
        )

        if self._config.get("api_key_url"):
            base_url += (
                f"?{self._config.get('api_key_header')}={self._config.get('api_key')}"
            )

        return base_url

    @property
    def endpoint(self) -> str:
        return ""

    @property
    def unified_schema(self) -> BaseModel:
        return None

    @property
    def custom_headers(self) -> dict:
        custom_headers = {
            "User-Agent": self._config.get("user_agent", "target-api <hello@hotglue.xyz>")
        }
        config_custom_headers = self._config.get("custom_headers") or list()
        for ch in config_custom_headers:
            if not isinstance(ch, dict):
                continue
            name = ch.get("name")
            value = ch.get("value")
            if not isinstance(name, str) or not isinstance(value, str):
                continue
            custom_headers[name] = value
        return custom_headers

    @property
    def is_full(self) -> bool:
        is_full_in_length = super().is_full
        is_full_in_bytes = False

        if self._config.get("max_size_in_bytes") and self._pending_batch:
            max_size_in_bytes = int(self._config.get("max_size_in_bytes"))
            batch_size_in_bytes = getsizeof(json.dumps(self._pending_batch["records"], cls=HGJSONEncoder))
            is_full_in_bytes = batch_size_in_bytes >= max_size_in_bytes * 0.9

        return is_full_in_length or is_full_in_bytes

    def response_error_message(self, response: requests.Response) -> str:
        try:
            response_text = f" with response body: '{response.text}'"[:5000]
        except:
            response_text = None

        request_url = response.request.url
        if self._config.get("api_key"):
            request_url = request_url.replace(self._config.get("api_key"), "__MASKED__")
        
        return f"Status code: {response.status_code} with {response.reason} for path: {request_url} {response_text}"
    
    def curlify_on_error(self, response):
        command = "curl -X {method} -H {headers} -d '{data}' '{uri}'"
        method = response.request.method
        uri = response.request.url
        data = response.request.body[:5000] if response.request.body else None

        if self._config.get("api_key_url"):
            uri = uri.replace(self._config.get("api_key"), "__MASKED__")

        headers = []
        api_key_header = (self._config.get("api_key_header") or "x-api-key").lower()

        for k, v in response.request.headers.items():
            # Mask the Authorization header
            if k.lower() == api_key_header:
                v = "__MASKED__"
            headers.append('"{0}: {1}"'.format(k, v))

        headers = " -H ".join(headers)
        return command.format(method=method, headers=headers, data=data, uri=uri)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if response.status_code in [429] or 500 <= response.status_code < 600:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.warning(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise RetriableAPIError(error)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            curl = self.curlify_on_error(response)
            self.logger.warning(f"cURL: {curl}")
            error = {"status_code": response.status_code, "body": msg}
            raise FatalAPIError(error)


    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params={}, request_data=None, headers={}, verify=True
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.default_headers)
        headers.update({"Content-Type": "application/json"})
        params.update(self.params)

        # changing data dumping to be able to send {} for when post_empty_record is true
        data = (
            json.dumps(request_data, cls=HGJSONEncoder)
            if request_data is not None
            else None
        )

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            data=data,
            verify=verify
        )
        self.validate_response(response)
        return response
    

    def init_state(self):
        # get the full target state
        target_state = self._target._latest_state

        # If there is data for the stream name in target_state use that to initialize the state
        if target_state:
            if not self._state and target_state["bookmarks"].get(self.name) and target_state["summary"].get(self.name):
                self.latest_state = target_state
        # If not init sink state latest_state
        if not self.latest_state:
            self.latest_state = self._state or {"bookmarks": {}, "summary": {}}

        if self.name not in self.latest_state["bookmarks"]:
            if not self.latest_state["bookmarks"].get(self.name):
                self.latest_state["bookmarks"][self.name] = []

        if not self.summary_init:
            self.latest_state["summary"] = {}
            if not self.latest_state["summary"].get(self.name):
                self.latest_state["summary"][self.name] = {"success": 0, "fail": 0, "existing": 0, "updated": 0}

            self.summary_init = True
