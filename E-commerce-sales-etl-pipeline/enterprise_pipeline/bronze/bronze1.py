import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from enterprise_pipeline.config import Configuration, SparkConfig
from enterprise_pipeline.connection.mysql_connection import read_table
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max, lit, to_timestamp
import logging
import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WatermarkController:
    """Handles the control table and watermark updates.

    This class keeps the metadata logic separate from raw data ingestion.
    In enterprise ETL, control metadata is managed independently from data reads.
    """

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb")
        self.control_table = self.dynamodb.Table("ecommerce_pipeline_control")

    def get_last_watermark(self, pipeline_table: str):
        """Fetch the last processed watermark for a specific source table.

        This preserves the existing logic: the pipeline compares new data against the
        last successful timestamp before writing to the bronze layer.
        """
        response = self.control_table.get_item(Key={"pipeline_table": pipeline_table})
        return response.get("Item", {}).get("last_successful_watermark")

    def update_watermark(self, pipeline_table: str, max_load_timestamp, new_data_count):
        """Update only the control metadata after a successful bronze load.

        This keeps audit information separate from the actual ingestion of raw data.
        """
        from datetime import datetime
        last_run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if isinstance(max_load_timestamp, datetime):
            max_load_timestamp = max_load_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        else:
            max_load_timestamp = str(max_load_timestamp)

        self.control_table.update_item(
            Key={"pipeline_table": pipeline_table},
            UpdateExpression="""
                                SET last_successful_watermark = :wm,
                                    last_run_status = :status,
                                    last_run_timestamp = :run_time,
                                    records_processed = :count
                                """,
            ExpressionAttributeValues={
                ":wm": max_load_timestamp,
                ":status": "SUCCESS",
                ":run_time": last_run_timestamp,
                ":count": new_data_count,
            },
        )

        logger.info(
            f"Control metadata updated for {pipeline_table} with watermark={max_load_timestamp}, "
            f"timestamp={last_run_timestamp}, records_processed={new_data_count}"
        )


class BronzeExtractor:
    """Responsible for reading raw source data and preparing the bronze dataset.

    This class focuses only on reading and filtering raw data. It does not manage
    pipeline control metadata; that is handled by WatermarkController.
    """

    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()
        self.watermark_controller = WatermarkController()

    def read_s3_raw_data(self, table: str, s3_path: str):
        """Read raw JSON from S3 and return a Spark DataFrame.

        This is the raw source extraction step in bronze architecture.
        """
        logger.info(f"Starting S3 bronze read for table: {table}")
        return self.start_spark.read.option("multiline", "true").json(s3_path)

    def read_mysql_raw_data(self, table: str):
        """Read raw table data from MySQL using the existing JDBC helper.

        This keeps the source read logic separate from loading and control updates.
        """
        logger.info(f"Starting MySQL bronze read for table: {table}")
        return read_table(self.start_spark, table, Configuration.mysql_config)

    def filter_incremental_rows(self, df: DataFrame, pipeline_table: str, timestamp_column: str = "last_updated_timestamp"):
        """Apply a timestamp-safe incremental filter used for Bronze ingestion.

        The key production requirement is that both the source timestamp column and the
        stored watermark are converted to the same timestamp type before comparison.
        """
        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in source data for {pipeline_table}.")

        last_watermark = self.watermark_controller.get_last_watermark(pipeline_table)
        logger.info(f"Last successful watermark for {pipeline_table}: {last_watermark}")

        if last_watermark is None:
            logger.info(f"No prior watermark found for {pipeline_table}. Loading all rows.")
            return df

        normalized_df = df.withColumn(timestamp_column, to_timestamp(col(timestamp_column)))
        normalized_watermark = to_timestamp(lit(str(last_watermark)))
        incremental_df = normalized_df.filter(col(timestamp_column) > normalized_watermark)
        logger.info(f"Filtered incremental rows for {pipeline_table}: {incremental_df.count()}")
        return incremental_df

    def write_bronze_data(self, df: DataFrame, s3_target_path: str):
        """Write raw data to the bronze path.

        This is still using the same behavior as the original pipeline: write the
        filtered DataFrame to parquet in bronze storage.
        """
        logger.info(f"Writing bronze data to: {s3_target_path}")
        df.write.mode("overwrite").parquet(s3_target_path)

    def ingest_data_from_s3(self, table: str, s3_path: str, s3_target_path: str, pipeline_table: str) -> DataFrame:
        """Bronze flow for S3-driven raw ingestion.

        This method keeps the original logic intact while making the separation clearer:
        1. read raw file
        2. filter by watermark
        3. write bronze data
        4. update metadata
        """
        logger.info(f"Starting reading S3 data for {table}")
        try:
            raw_df = self.read_s3_raw_data(table, s3_path)
            incremental_df = self.filter_incremental_rows(raw_df, pipeline_table)
            new_data_count = incremental_df.count()
            logger.info(f"Incremental data count for {table}: {new_data_count}")

            if new_data_count > 0:
                max_load_timestamp = incremental_df.agg(
                    max(col("last_updated_timestamp")).alias("max_load_timestamp")
                ).collect()[0]["max_load_timestamp"]

                self.write_bronze_data(incremental_df, s3_target_path)
                self.watermark_controller.update_watermark(
                    pipeline_table=pipeline_table,
                    max_load_timestamp=max_load_timestamp,
                    new_data_count=new_data_count,
                )
            else:
                logger.info(f"No incremental data found for {table}. Bronze write skipped.")

            return incremental_df
        except Exception as e:
            logger.error(f"Error found while loading incremental S3 data for {table}: {e}")
            raise

    def ingest_data_from_mysql_database(self, table: str, s3_target_path: str, pipeline_table: str) -> DataFrame:
        """Bronze flow for MySQL-driven raw ingestion.

        This preserves the original behavior but separates extraction from control logic.
        """
        logger.info(f"Starting reading MySQL data for {table}")
        try:
            raw_df = self.read_mysql_raw_data(table)
            incremental_df = self.filter_incremental_rows(raw_df, pipeline_table)
            new_data_count = incremental_df.count()
            logger.info(f"Incremental data count for {table}: {new_data_count}")

            if new_data_count > 0:
                max_load_timestamp = incremental_df.agg(
                    max(col("last_updated_timestamp")).alias("max_load_timestamp")
                ).collect()[0]["max_load_timestamp"]

                self.write_bronze_data(incremental_df, s3_target_path)
                self.watermark_controller.update_watermark(
                    pipeline_table=pipeline_table,
                    max_load_timestamp=max_load_timestamp,
                    new_data_count=new_data_count,
                )
            else:
                logger.info(f"No incremental data found for {table}. Bronze write skipped.")

            return incremental_df
        except Exception as e:
            logger.error(f"Error found while loading incremental MySQL data for {table}: {e}")
            raise


if __name__ == "__main__":
    """This module contains the Bronze extraction logic.

    Use bronze_runner.py as the job entrypoint when running the full Bronze ingestion process.
    """
    print("BronzeExtractor loaded. Use bronze_runner.py as the job entry point.")
