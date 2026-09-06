import os
from datetime import datetime
from dotenv import load_dotenv


def _resolve_project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class PipelineConfig:
    """Project-aware ETL configuration aligned with the working Bronze/Silver logic."""

    project_root = _resolve_project_root()
    env_path = os.path.join(project_root, "Env_variable", "credentials.env")
    load_dotenv(env_path)

    mysql_config = {
        "host": os.getenv("host", "host.docker.internal"),
        "port": os.getenv("port", "3306"),
        "database": os.getenv("database", "ecommerce"),
        "username": os.getenv("username", "root"),
        "password": os.getenv("password", ""),
        "driver": os.getenv("driver", "com.mysql.cj.jdbc.Driver"),
    }

    bucket = os.getenv("s3_bucket", "e-commerce-sales-etl")
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
