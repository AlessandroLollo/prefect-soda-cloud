import base64
from datetime import datetime
from time import sleep
from typing import List, Optional

from requests import Session


class SodaCloudClient:

    __TRIGGER_SCAN_V1_ENDPOINT = "api/v1/scans"
    __GET_SCAN_STATUS_V1_ENDPOINT = "api/v1/scans/{scanId}"
    __GET_SCAN_LOGS_V1_ENDPOINT = "api/v1/scans/{scanId}/logs"

    def __init__(self, api_base_url: str, username: str, password: str) -> None:
        """
        Create a `SodaCloudClient` object that can be used to interact
        with Soda Cloud APIs.
        """
        self.__api_base_url = api_base_url
        self.__username = username
        self.__password = password
        self.__session = self.__get_session()

    def __get_auth_header(self) -> str:
        """
        Return the value of the authorization header computed
        using the username and password
        """
        secret = f"{self.__username}:{self.__password}".encode("utf-8")
        return base64.b64encode(secret).decode("utf-8")

    def __get_session(self) -> Session:
        """
        Create a `Session` object to interact with Soda Cloud APIs.
        """
        session = Session()
        auth_header = self.__get_auth_header()
        session.headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": f"Basic {auth_header}",
        }
        return session

    def trigger_scan(self, scan_name: str, data_timestamp: Optional[datetime]) -> str:
        """
        Trigger a Soda Scan given its name and return the corresponding Scan ID.
        Uses [Trigger Scan](https://docs.soda.io/api-docs/public-cloud-api-v1.html#/operations/POST/api/v1/scans)
        """
        url = f"{self.__api_base_url}/{self.__TRIGGER_SCAN_V1_ENDPOINT}"
        payload = {"scanDefinition": scan_name}
        if data_timestamp:
            payload["dataTimestamp"] = data_timestamp.isoformat()

        with self.__session.post(url=url, data=payload) as response:
            if response.status_code != 201:
                raise Exception(response.json())

        return response.headers["X-Soda-Scan-Id"]

    def get_scan_status(self, scan_id: str) -> dict:
        """
        Return dict that contains information about the run status
        of the scan given the corresponding Scan ID.
        Uses [Get Scan Status](https://docs.soda.io/api-docs/public-cloud-api-v1.html#/operations/GET/api/v1/scans/{scanId})
        """
        get_scan_status_endpoint = self.__GET_SCAN_STATUS_V1_ENDPOINT.format(
            scanId=scan_id
        )
        url = f"{self.__api_base_url}/{get_scan_status_endpoint}"

        with self.__session.get(url=url) as response:
            if response.status_code != 200:
                raise Exception(response.json())

        return response.json()

    def get_scan_logs(self, scan_id: str) -> List[dict]:
        """
        TODO
        """
        get_scan_logs_endpoint = self.__GET_SCAN_LOGS_V1_ENDPOINT.format(scanId=scan_id)
        url = f"{self.__api_base_url}/{get_scan_logs_endpoint}"

        is_scan_complete = False

        while not is_scan_complete:
            scan_status = self.get_scan_status(scan_id=scan_id)
            if "ended" not in scan_status:
                print("Waiting for scan to finish...")
                sleep(5)
            else:
                is_scan_complete = True

        content = []
        is_last_log_message = False

        while not is_last_log_message:
            with self.__session.get(url=url) as response:
                if response.status_code != 200:
                    raise Exception(response.json())

            data = response.json()
            content.extend(data["content"])
            is_last_log_message = data["last"]

            if not is_last_log_message:
                print("Waiting for more logs...")
                sleep(5)

        return content
