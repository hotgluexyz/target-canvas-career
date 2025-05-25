"""CanvasCareer target sink class, which handles writing streams."""

from __future__ import annotations
from target_canvas_career.client import CanvasCareerSink
import requests
from io import StringIO
import csv
import backoff
import requests
import json
from typing import Any, Dict, Optional
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.auth import Authenticator
from target_hotglue.common import HGJSONEncoder


class ImportSink(CanvasCareerSink):
    """CanvasCareer target sink class."""

    name = "Import"

    @property
    def endpoint(self):
        return f"/accounts/{self.config.get('account_id')}/sis_imports"
    
    @property
    def http_headers(self):
        headers = super().http_headers
        headers["Content-Type"] = "application/octet-stream"
        return headers
    
    def preprocess_record(self, record: dict, context: dict) -> None:

        # Validate attachment exists in record
        if 'attachment' not in record:
            return {"error": "Record must contain 'attachment' field"}

        # Create multipart form data with zip file
        files ={
            'filename': record['attachment'],
            'path': f"{self.config.get('input_path')}/{record['attachment']}"
        }
        return files
    
    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record.get("error"))
        
        params = {
            "import_type": "instructure_csv",
            "extension": "zip"
        }

        response = self.request_api(
            "POST", endpoint=self.endpoint, request_data=record, params=params
        )
        import_id = response.json()["id"]

        # check if the import completed without errors
        import_status = self.request_api(
            "GET", endpoint=f"{self.endpoint}/{import_id}"
        )
        if import_status.json().get("processing_warnings"):
            raise Exception(f"Import {import_id} failed with warnings: {import_status.json()['processing_warnings']}")
        
        return import_id, True, state_updates

    
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
        headers.update(self.http_headers)
        params.update(self.params)

        with open(request_data['path'], "rb") as f:
            file_data = f.read()

        with open(request_data['path'], 'rb') as f:
            files = {
                'attachment': f
            }

        response = requests.post(url, headers=headers, params=params, data=file_data)
        self.validate_response(response)
        return response