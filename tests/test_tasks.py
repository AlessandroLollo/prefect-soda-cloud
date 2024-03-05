from urllib.parse import urljoin

import pytest
import responses
from prefect import flow

from prefect_soda_cloud import SodaCloudAuthConfig, SodaCloudCredentials
from prefect_soda_cloud.exceptions import TriggerScanException
from prefect_soda_cloud.tasks import trigger_scan

creds = SodaCloudCredentials(user_or_api_key_id="test", pwd_or_api_key_secret="test")

auth_config = SodaCloudAuthConfig(
    api_base_url="https://test.test", creds=creds, wait_secs_between_api_calls=1
)


@responses.activate
def test_trigger_scan_fails():
    api_base_url = "https://test.test"
    trigger_scan_endpoint = "api/v1/scans"
    url = urljoin(api_base_url, trigger_scan_endpoint)

    @flow(name="trigger_scan_fails")
    def test_flow():
        return trigger_scan(
            scan_name="test_scan",
            soda_cloud_auth_config=auth_config,
            data_timestamp=None,
        )

    responses.add(responses.POST, url, status=500, json={"error": "Error!"})

    with pytest.raises(TriggerScanException, match=str({"error": "Error!"})):
        test_flow()


@responses.activate
def test_trigger_scan_succeed():
    api_base_url = "https://test.test"
    trigger_scan_endpoint = "api/v1/scans"
    url = urljoin(api_base_url, trigger_scan_endpoint)

    @flow(name="trigger_scan_succeed")
    def test_flow():
        return trigger_scan(
            scan_name="test_scan",
            soda_cloud_auth_config=auth_config,
            data_timestamp=None,
        )

    responses.add(responses.POST, url, status=201, headers={"X-Soda-Scan-Id": "abc"})

    result = test_flow()
    assert result == "abc"


@responses.activate
def test_trigger_scan_with_wait_succeed():
    api_base_url = "https://test.test"
    trigger_scan_endpoint = "api/v1/scans"
    trigger_scan_url = urljoin(api_base_url, trigger_scan_endpoint)

    scan_id = "abc"
    get_scan_status_endpoint = f"api/v1/scans/{scan_id}"
    get_scan_status_url = urljoin(api_base_url, get_scan_status_endpoint)

    @flow(name="trigger_scan_succeed")
    def test_flow():
        return trigger_scan(
            scan_name="test_scan",
            soda_cloud_auth_config=auth_config,
            data_timestamp=None,
            wait_for_scan_end=True,
        )

    responses.add(
        responses.POST, trigger_scan_url, status=201, headers={"X-Soda-Scan-Id": "abc"}
    )

    responses.add(
        responses.GET,
        get_scan_status_url,
        status=200,
        json={
            "agentId": "the_agent",
            "checks": [{"evaluationStatus": "pass", "id": "the_check"}],
            "cloudUrl": "the_url",
            "created": "the_creation_time",
            "ended": "the_ended_time",
            "errors": 0,
            "failures": 0,
            "id": scan_id,
            "scanDefinition": {"id": "the_scan_definition_id", "name": "the_scan_name"},
            "scanTime": "the_scan_time",
            "started": "the_start_time",
            "state": "queuing",
            "submitted": "the_submitted_time",
            "warnings": 0,
        },
    )

    result = test_flow()
    assert result == "abc"


@responses.activate
def test_trigger_scan_with_return_logs_succeed():
    api_base_url = "https://test.test"
    trigger_scan_endpoint = "api/v1/scans"
    trigger_scan_url = urljoin(api_base_url, trigger_scan_endpoint)

    scan_id = "abc"
    get_scan_status_endpoint = f"api/v1/scans/{scan_id}"
    get_scan_status_url = urljoin(api_base_url, get_scan_status_endpoint)

    get_scan_logs_endpoint = f"api/v1/scans/{scan_id}/logs"
    get_scan_logs_url = urljoin(api_base_url, get_scan_logs_endpoint)

    @flow(name="trigger_scan_succeed")
    def test_flow():
        return trigger_scan(
            scan_name="test_scan",
            soda_cloud_auth_config=auth_config,
            data_timestamp=None,
            return_logs=True,
        )

    responses.add(
        responses.POST, trigger_scan_url, status=201, headers={"X-Soda-Scan-Id": "abc"}
    )

    responses.add(
        responses.GET,
        get_scan_status_url,
        status=200,
        json={
            "agentId": "the_agent",
            "checks": [{"evaluationStatus": "pass", "id": "the_check"}],
            "cloudUrl": "the_url",
            "created": "the_creation_time",
            "ended": "the_ended_time",
            "errors": 0,
            "failures": 0,
            "id": scan_id,
            "scanDefinition": {"id": "the_scan_definition_id", "name": "the_scan_name"},
            "scanTime": "the_scan_time",
            "started": "the_start_time",
            "state": "queuing",
            "submitted": "the_submitted_time",
            "warnings": 0,
        },
    )

    responses.add(
        responses.GET,
        get_scan_logs_url,
        status=200,
        json={"content": [{"row1": "log1"}, {"row2": "log2"}], "last": True},
    )

    result = test_flow()
    assert result == [{"row1": "log1"}, {"row2": "log2"}]
