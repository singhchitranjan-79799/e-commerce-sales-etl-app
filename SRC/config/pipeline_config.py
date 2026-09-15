import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions


# AWS Glue Job Parameters
# These values are supplied to the Glue job as:
# --host, --port, --database, --username, --password, --driver, --s3_bucket
args = getResolvedOptions(
    sys.argv,
    [
        "host",
        "port",
        "database",
        "username",
        "password",
        "driver",
        "s3_bucket",
    ],
)


class PipelineConfig:
    """ETL configuration for AWS Glue runtime."""

    mysql_config = {
        "host": args["host"],
        "port": args["port"],
        "database": args["database"],
        "username": args["username"],
        "password": args["password"],
        "driver": args["driver"],
    }

    bucket = args["s3_bucket"]
    prefix = "ingestion_data"
    base_path = f"s3://{bucket}/{prefix}"

    bronze_prefix = "bronze_data"
    bronze_path = f"s3://{bucket}/{bronze_prefix}"

    silver_prefix = "silver_data"
    silver_path = f"s3://{bucket}/{silver_prefix}"

    timestamp = datetime.now().strftime("%Y%m%d")

    table_name = [
        {
            "source_type": "my_sql",
            "source_table": "customers",
            "target_table": "customer_data",
        },
        {
            "source_type": "my_sql",
            "source_table": "products",
            "target_table": "product_data",
        },
        {
            "source_type": "s3",
            "source_table": "inventory_data",
            "target_table": "inventory_data",
        },
        {
            "source_type": "s3",
            "source_table": "order_item_data",
            "target_table": "order_item_data",
        },
        {
            "source_type": "s3",
            "source_table": "orders_data",
            "target_table": "orderss_data",
        },
    ]

    bronze_customer_prefix = f"{bronze_prefix}/customer_data"
    silver_customer_prefix = f"{silver_prefix}/customer_data"
    control_table = "ecommerce_pipeline_control"


Configuration = PipelineConfig

__all__ = ["Configuration", "PipelineConfig"]
