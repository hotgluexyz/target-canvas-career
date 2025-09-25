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
        headers = {"Content-Type": "application/octet-stream"}

        response = self.request_api(
            "POST",
            endpoint=self.endpoint,
            request_data=record,
            params=params,
            headers=headers,
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
        if not provisioning_report_json.get("attachment", {}).get("url"):
            raise Exception(
                f"Report {provisioning_report_id} to endpoint {provisioning_report_path} failed to generate provisioning report. Error: {provisioning_report_json}"
            )

        # get user uuids from provisioning report
        self.get_user_uuids(provisioning_report_json["attachment"]["url"])

        if self.warnings:
            return import_id, False, {"error": self.warnings}
        return import_id, True, state_updates

    def get_user_uuids(self, report_url: str) -> None:
        # get user uuids from provisioning report
        # read report url and get user uuids
        response = requests.get(report_url, headers=self.http_headers)
        csv_content = response.content.decode("utf-8")
        user_uuids = list(csv.DictReader(csv_content.splitlines()))
        # get only user id and uuid
        user_uuids = {str(user["user_id"]): user["uuid"] for user in user_uuids}
        self._target.user_uuids = user_uuids

    def create_provisioning_report(self, provisioning_report_path: str) -> dict:
        request_data = {
            "parameters[include_deleted]": (None, "false"),
            "parameters[users]": (None, "true"),
            "parameters[skip_message]": (None, "true"),
        }

        provisioning_report = self.request_api(
            "POST",
            endpoint=provisioning_report_path,
            files=request_data,
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

    def request_api(
        self,
        http_method,
        endpoint=None,
        params={},
        request_data=None,
        headers={},
        verify=True,
        files=None,
    ):
        """Request records from REST endpoint(s), returning response records."""
        resp = self._request(
            http_method,
            endpoint,
            params,
            request_data,
            headers,
            verify=verify,
            files=files,
        )
        return resp

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
        files=None,
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers.update(self.http_headers)
        params.update(self.params)

        # needed to send the zip file data
        if isinstance(request_data, dict) and "path" in request_data:
            with open(request_data["path"], "rb") as f:
                file_data = f.read()
        else:
            file_data = request_data

        response = requests.request(
            http_method,
            url,
            headers=headers,
            params=params,
            data=file_data,
            files=files,
        )
        self.validate_response(response)
        return response


class MetadataSink(CanvasCareerGraphQLSink):
    """CanvasCareer target sink class."""

    name = "users_metadata"
    warnings = []

    @property
    def url_base(self) -> str:
        """Return the API URL base, defaulting to the standard REST API base."""
        return self.config.get("metadata_service_tenant")

    @property
    def endpoint(self):
        return f"/graphql"

    def process_batch_record(self, record: dict, context: dict) -> dict:
        canvas_user_uuid = record.get("canvasUserUuid")
        leader_canvas_user_uuid = record.get("leaderCanvasUserUuid")

        # get user uuids from user_uuids if not passed
        if record.get("user_id") and not canvas_user_uuid:
            user_id = str(record["user_id"])
            canvas_user_uuid = self._target.user_uuids.get(user_id)
        
        # we can't send metadata without the user uuid
        if not canvas_user_uuid:
             return {
                "externalId": record["user_id"],
                "error": f"User {record.get('user_id')} not found in provisioning report, for more details check in import warnings why this user failed to import",
            }

        # get metadata leader uuids from user_uuids if not passed
        if record.get("metadata_leader_id") and not leader_canvas_user_uuid:
            leader_canvas_user_uuid = self._target.user_uuids.get(
                record["metadata_leader_id"]
            )
            if not leader_canvas_user_uuid:
                return {
                    "externalId": record["user_id"],
                    "error": f"User {record.get('metadata_leader_id')} not found in provisioning report, for more details check in import warnings why this user failed to import",
                }
            else:
                leader_canvas_user_uuid = [leader_canvas_user_uuid]

        # build payload
        payload = {
            "canvasRootAccountUuid": record.get("canvasRootAccountUuid"),
            "canvasUserUuid": canvas_user_uuid,
            "leaderCanvasUserUuids": leader_canvas_user_uuid,
            "metadata": [{"key": field, "value": record.get(field)} for field in ["organization", "department", "team", "role"] if record.get(field)],
        }
        return payload

    def make_batch_request(self, records: List[dict]):
        self.logger.info(f"Processing {self.stream_name}")

        if not records:
            self.logger.info(f"No records to process")
            return

        failed_records = [record for record in records if record.get("error")]
        payload_records = [record for record in records if not record.get("error")]

        query = """
        mutation BulkUpsertMetadata{
            bulkUpsertMetadata(input: {
                users: __records__
            }) {
                users {
                    id
                    canvasUserUuid
                }
            }
        }
        """

        query = query.replace("__records__", self.to_graphql_input(payload_records))
        query = " ".join(query.strip().split())
        
        try:    
            invoicebatch_response = self.request_api(
                "POST", endpoint=self.endpoint, request_data={"query": query}
            )
        except Exception as e:
            return failed_records, e.args[0]

        return failed_records, invoicebatch_response

    def handle_batch_response(self, failed_records, response) -> dict:
        state = []

        for failed_record in failed_records:
            state.append(
                {
                    "success": False,
                    "error": failed_record.get("error"),
                    "externalId": failed_record.get("externalId"),
                }
            )

        response_json = response.json()
        if response.status_code == 200 and not "errors" in response_json:
            for user in (
                response_json.get("data", {})
                .get("bulkUpsertMetadata", {})
                .get("users", [])
            ):
                # create state for each user
                uuid = user.get("canvasUserUuid")
                state.append(
                    {
                        "id": uuid,
                        "success": True,
                        "externalId": next(
                            (
                                k
                                for k, v in self._target.user_uuids.items()
                                if v == uuid
                            ),
                            None,
                        ),
                    }
                )
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
