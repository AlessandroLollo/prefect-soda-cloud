"""This is an example flows module"""
from prefect import flow

from prefect_soda_cloud.blocks import SodacloudBlock
from prefect_soda_cloud.tasks import (
    goodbye_prefect_soda_cloud,
    hello_prefect_soda_cloud,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    SodacloudBlock.seed_value_for_example()
    block = SodacloudBlock.load("sample-block")

    print(hello_prefect_soda_cloud())
    print(f"The block's value: {block.value}")
    print(goodbye_prefect_soda_cloud())
    return "Done"


if __name__ == "__main__":
    hello_and_goodbye()
