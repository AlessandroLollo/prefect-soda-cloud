from prefect import flow

from prefect_soda_cloud.tasks import (
    goodbye_prefect_soda_cloud,
    hello_prefect_soda_cloud,
)


def test_hello_prefect_soda_cloud():
    @flow
    def test_flow():
        return hello_prefect_soda_cloud()

    result = test_flow()
    assert result == "Hello, prefect-soda-cloud!"


def goodbye_hello_prefect_soda_cloud():
    @flow
    def test_flow():
        return goodbye_prefect_soda_cloud()

    result = test_flow()
    assert result == "Goodbye, prefect-soda-cloud!"
