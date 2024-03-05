from datetime import datetime
from typing import List, Optional, Union

from prefect import get_run_logger, task

from prefect_soda_cloud import SodaCloudAuthConfig


@task
def trigger_scan(
    scan_name: str,
    soda_cloud_auth_config: SodaCloudAuthConfig,
    data_timestamp: Optional[datetime] = None,
    wait_for_scan_end: bool = False,
    return_logs: bool = False,
) -> Union[List[dict], str]:
    """
    Trigger a scan given its name.

    Args:
        scan_name: The name of the scan to trigger.
        soda_cloud_auth_config: The auth configuration to use to trigger the scan.
        wait_for_scan_end: Whether to wait for the scan execution to finish or not.
        return_logs: Whether to return the scan execution logs instead of
            the scan identifier.

    Returns:
        If `wait_for_scan_end = False`, returns the Scan ID.
        If `wait_for_scan_end = True and return_logs = False`, returns the Scan ID.
        If `wait_for_scan_end = True and return_logs = True`, returns scan logs.
    """
    soda_cloud_client = soda_cloud_auth_config.get_client(logger=get_run_logger())
    scan_id = soda_cloud_client.trigger_scan(
        scan_name=scan_name, data_timestamp=data_timestamp
    )

    if return_logs:
        return soda_cloud_client.get_scan_logs(scan_id=scan_id)

    elif wait_for_scan_end:
        soda_cloud_client.get_scan_status(
            scan_id=scan_id, wait_for_scan_end=wait_for_scan_end
        )
        return scan_id

    else:
        return scan_id


@task
def get_scan_status(
    scan_id: str,
    soda_cloud_auth_config: SodaCloudAuthConfig,
    wait_for_scan_end: bool = False,
) -> dict:
    """
    Retrieve scan status from Soda Cloud given the scan ID.

    Args:
        scan_id: Scan identifier provided by Soda Cloud.
        soda_cloud_auth_config: The auth configuration to use to trigger the scan.
        wait_for_scan_end: Whether to wait for the scan execution to finish or not.

    Returns:
        A dictionary that describes the status of the scan.
    """
    soda_cloud_client = soda_cloud_auth_config.get_client(logger=get_run_logger())
    scan_status = soda_cloud_client.get_scan_status(
        scan_id=scan_id, wait_for_scan_end=wait_for_scan_end
    )
    return scan_status


@task
def get_scan_logs(
    scan_id: str, soda_cloud_auth_config: SodaCloudAuthConfig
) -> List[dict]:
    """
    Retrieve scan logs from Soda Cloud given the scan ID.

    Args:
        scan_id: Scan identifier provided by Soda Cloud.
        soda_cloud_auth_config: The auth configuration to use to trigger the scan.

    Returns:
        A list of dict, each dict being a Soda Cloud log message.
    """
    soda_cloud_client = soda_cloud_auth_config.get_client(logger=get_run_logger())
    scan_logs = soda_cloud_client.get_scan_logs(scan_id=scan_id)
    return scan_logs
