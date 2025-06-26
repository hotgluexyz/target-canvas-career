"""CanvasCareer target sink class, which handles writing streams."""

from __future__ import annotations
from target_canvas_career.client import CanvasCareerSink, CanvasCareerGraphQLSink
import requests
import backoff
import requests
from singer_sdk.exceptions import RetriableAPIError
import time
from requests_toolbelt import MultipartEncoder
import csv
from typing import List
import json


class ImportSink(CanvasCareerSink):
    """CanvasCareer target sink class."""

    name = "Import"
    warnings = []

    @property
    def base_endpoint(self):
        return f"/accounts/{self.config.get('account_id')}"

    @property
    def endpoint(self):
        return f"{self.base_endpoint}/sis_imports"

    @property
    def http_headers(self):
        headers = super().http_headers
        headers["Content-Type"] = "application/octet-stream"
        return headers

    def preprocess_record(self, record: dict, context: dict) -> None:
        # Validate attachment exists in record
        if "attachment" not in record:
            return {"error": "Record must contain 'attachment' field"}

        # Create multipart form data with zip file
        files = {
            "filename": record["attachment"],
            "path": f"{self.config.get('input_path')}/{record['attachment']}",
        }
        return files

    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        state_updates = dict()
        if record.get("error"):
            raise Exception(record.get("error"))

        params = {"import_type": "instructure_csv", "extension": "zip"}

        response = self.request_api(
            "POST", endpoint=self.endpoint, request_data=record, params=params
        )
        import_id = response.json()["id"]

        import_status_json = self.poll_report_completion(
            import_id, endpoint=self.endpoint
        )

        if import_status_json.get("processing_warnings"):
            self.warnings = import_status_json["processing_warnings"]
            # warnings = (f"Import {import_id} failed with warnings: {import_status.json()['processing_warnings']}")

        # After import is complete fetch user uuids, if any user failed to import raise exception at the end of the job7
        provisioning_report_path = f"{self.base_endpoint}/reports/provisioning_csv"
        provisioning_report_id = self.create_provisioning_report(
            provisioning_report_path
        )
        provisioning_report_json = self.poll_report_completion(
            provisioning_report_id, endpoint=provisioning_report_path
        )

        # get user uuids from provisioning report
        report_url = provisioning_report_json.get("file_url")
        # read report url and get user uuids
        response = requests.get(report_url)
        csv_content = response.content.decode("utf-8")
        user_uuids = csv.DictReader(csv_content.splitlines())
        self._target.user_uuids = user_uuids

        # check if any user failed to import
        for user in user_uuids:
            if user.get("status") == "failed":
                raise Exception(f"User {user.get('user_id')} failed to import")

        return import_id, True, state_updates

    def create_provisioning_report(self, provisioning_report_path: str) -> dict:
        request_data = MultipartEncoder(
            fields={
                "parameters[include_deleted]": "false",
                "parameters[skip_message]": "true",
                "parameters[users]": "true",
            }
        )
        headers = {
            "Content-Type": request_data.content_type,
        }
        provisioning_report = self.request_api(
            "POST",
            endpoint=provisioning_report_path,
            request_data=request_data,
            headers=headers,
        )
        return provisioning_report.json()["id"]

    def poll_report_completion(self, report_id: str, endpoint: str) -> dict:
        import_status_json = {}
        timeout = 120
        sleep_time = 3
        while import_status_json.get("progress") != 100:
            if timeout <= 0:
                raise Exception(f"Report {report_id} to endpoint {endpoint} timed out")
            time.sleep(3)
            timeout -= sleep_time
            import_status = self.request_api("GET", endpoint=f"{endpoint}/{report_id}")
            self.logger.info(
                f"Import {report_id} is not complete, waiting for completion..."
            )
            import_status_json = import_status.json()
        return import_status_json

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self,
        http_method,
        endpoint,
        params={},
        request_data=None,
        headers={},
        verify=True,
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.http_headers)
        params.update(self.params)

        if isinstance(request_data, dict) and "path" in request_data:
            with open(request_data["path"], "rb") as f:
                file_data = f.read()
        else:
            file_data = request_data

        response = requests.request(
            http_method, url, headers=headers, params=params, data=file_data
        )
        self.validate_response(response)
        return response


class MetadataSink(CanvasCareerGraphQLSink):
    """CanvasCareer target sink class."""

    name = "users_metadata"
    warnings = []

    @property
    def endpoint(self):
        return f"/graphql"

    def process_batch_record(self, record: dict, context: dict) -> dict:
        # get user uuids from if not passed
        if record.get("user_id") and not record.get("canvasUserUuid"):
            canvas_user_uuid = self._target.user_uuids.get(record["user_id"])
            if not canvas_user_uuid:
                raise Exception(
                    f"User {record.get('user_id')} not found in provisioning report"
                )  # TODO: add error to state_updates

        if record.get("metadata_leader_id") and not record.get("leaderCanvasUserUuid"):
            leader_canvas_user_uuid = self._target.user_uuids.get(
                record["metadata_leader_id"]
            )
            if not leader_canvas_user_uuid:
                raise Exception(
                    f"User {record.get('metadata_leader_id')} not found in provisioning report"
                )

        # build payload
        payload = {
            "canvasRootAccountUuid": record.get("canvasRootAccountUuid"),
            "canvasUserUuid": record.get("canvasUserUuid"),
            "leaderCanvasUserUuid": record.get("leaderCanvasUserUuid"),
            "metadata": [
                {"key": "organization", "value": record.get("organization")},
                {"key": "department", "value": record.get("department")},
                {"key": "team", "value": record.get("team")},
                {"key": "role", "value": record.get("role")},
            ],
        }
        return payload

    def make_batch_request(self, records: List[dict]):
        self.logger.info(f"Processing {self.stream_name}")

        if not records:
            self.logger.info(f"No records to process")
            return

        payload = """
        mutation BulkUpsertMetadata{
            bulkUpsertMetadata(input: {
                users: __records__
            }) {
                users {
                    id
                }
            }
        }
        """

        payload = payload.replace("__records__", json.dumps(records))

        invoicebatch_response = self.request_api(
            "POST", endpoint=self.endpoint, request_data=payload
        )
        return invoicebatch_response

    def handle_batch_response(self, response) -> dict:
        response_json = response.json()
        if response.status_code == 201:
            state = []
            for user in []:
                # create state for each user
                state.append({})
            return {"state_updates": state}
        else:
            return {
                "state_updates": [
                    {
                        "success": False,
                        "error": f"Batch failure with response: {response_json}",
                    }
                ]
            }
