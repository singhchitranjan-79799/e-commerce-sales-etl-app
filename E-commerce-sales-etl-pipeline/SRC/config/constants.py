from enterprise_pipeline.config.pipeline_config import PipelineConfig

BRONZE_PREFIX = "bronze_data"
SILVER_PREFIX = "silver_data"
S3_BUCKET = PipelineConfig.bucket
CONTROL_TABLE = PipelineConfig.control_table
TABLE_MAP = PipelineConfig.table_name

BRONZE_CUSTOMER_PREFIX = PipelineConfig.bronze_customer_prefix
SILVER_CUSTOMER_PREFIX = PipelineConfig.silver_customer_prefix

__all__ = [
    "BRONZE_PREFIX",
    "SILVER_PREFIX",
    "S3_BUCKET",
    "CONTROL_TABLE",
    "TABLE_MAP",
    "BRONZE_CUSTOMER_PREFIX",
    "SILVER_CUSTOMER_PREFIX",
]
