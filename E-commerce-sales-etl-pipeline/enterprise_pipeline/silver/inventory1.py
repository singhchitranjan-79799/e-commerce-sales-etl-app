import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTERPRISE_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT, ENTERPRISE_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from enterprise_pipeline.config import Configuration, SparkConfig
import boto3
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col,
    current_date,
    when,
    trim,
)
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Inventory:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()

    inventory_schema = StructType([
        StructField("inventory_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("warehouse_name", StringType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
        StructField("last_stock_update", DateType(), True),
        StructField("inventory_status", StringType(), True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ])

    bronze_inventory_prefix = "bronze_data/inventory_data"
    silver_inventory_prefix = "silver_data/inventory_data"

    def extract_bronze_table_partition(self):
        try:
            s3 = boto3.client("s3")
            bucket = Configuration.bucket
            list_object = s3.list_objects_v2(Bucket=bucket, Prefix=self.bronze_inventory_prefix)

            bronze_partition = set()
            for obj in list_object.get("Contents", []):
                key = obj["Key"]
                if key == "bronze_data/inventory_data/":
                    continue
                folder = key.split("/")[2]
                bronze_partition.add(folder)

            bronze_partition = sorted(bronze_partition)
            logger.info(f"Detected bronze partitions for inventory data: {bronze_partition}")
            return bronze_partition
        except Exception as e:
            logger.error(f"Failed to fetch bronze partitions for inventory data: {e}")
            raise

    def extract_silver_table_partition(self):
        try:
            s3 = boto3.client("s3")
            unprocessed_partition = s3.list_objects_v2(
                Bucket=Configuration.bucket,
                Prefix=self.silver_inventory_prefix,
            )

            silver_partition = set()
            for object in unprocessed_partition.get("Contents", []):
                key = object["Key"]
                if key == "silver_data/inventory_data/":
                    continue
                part = key.split("/")
                if len(part) < 3 or not part[2]:
                    continue
                silver_partition.add(part[2])

            latest_partition = sorted(silver_partition)[-1] if silver_partition else None
            logger.info(f"Latest silver partition for inventory data: {latest_partition}")
            return latest_partition
        except Exception as e:
            logger.error(f"Failed to determine latest silver partition for inventory data: {e}")
            raise

    def bronze_unprocessed_partition(self, bronze_partition, latest_partition):
        try:
            pending_partition = []
            if not latest_partition:
                pending_partition.extend(bronze_partition)
            else:
                for partition in bronze_partition:
                    if partition > latest_partition:
                        pending_partition.append(partition)
            logger.info(f"Pending bronze partitions to process for inventory data: {pending_partition}")
            return pending_partition
        except Exception as e:
            logger.error(f"Failed to determine unprocessed bronze partitions: {e}")
            raise

    def schema_validation(self, s3_path: str):
        try:
            logger.info(f"Starting schema validation for bronze path: {s3_path}")
            df = self.start_spark.read.option("inferSchema", "true").parquet(s3_path)
            validation_results = {
                "missing_field": [],
                "extra_field": [],
                "nullability_mismatch": [],
            }

            for field in self.inventory_schema:
                if field.name not in df.columns:
                    validation_results["missing_field"].append(field.name)
                else:
                    expected_nullable_value = field.nullable
                    actual_nullable_value = df.schema[field.name].nullable
                    if expected_nullable_value != actual_nullable_value:
                        validation_results["nullability_mismatch"].append({
                            "field_name": field.name,
                            "expected_nullable_value": expected_nullable_value,
                            "actual_nullable_value": actual_nullable_value,
                        })

            for field in df.columns:
                if field not in self.inventory_schema.fieldNames():
                    validation_results["extra_field"].append(field)

            logger.info(f"Schema validation result for {s3_path}: {validation_results}")
            if validation_results["missing_field"] or validation_results["extra_field"] or validation_results["nullability_mismatch"]:
                raise ValueError(f"Schema validation failed for {s3_path}: {validation_results}")
            return df
        except Exception as e:
            logger.error(f"Schema validation error for {s3_path}: {e}")
            raise

    def load_product_reference(self):
        try:
            product_prefix = "silver_data/product_data"
            s3 = boto3.client("s3")
            objects = s3.list_objects_v2(Bucket=Configuration.bucket, Prefix=product_prefix)
            partitions = []
            for obj in objects.get("Contents", []):
                key = obj["Key"]
                if key == "silver_data/product_data/":
                    continue
                part = key.split("/")
                if len(part) >= 3 and part[2]:
                    partitions.append(part[2])

            if not partitions:
                raise ValueError("No silver product partitions found to use as reference data.")

            latest_partition = sorted(partitions)[-1]
            product_path = f"s3://{Configuration.bucket}/{product_prefix}/{latest_partition}"
            logger.info(f"Loading product reference data from: {product_path}")
            df_product = self.start_spark.read.option("inferSchema", "true").parquet(product_path)
            logger.info(f"Product reference loaded successfully with {df_product.count()} rows")
            return df_product
        except Exception as e:
            logger.error(f"Failed to load product reference data: {e}")
            raise

    def run_dq_check(self, df, df_product):
        try:
            logger.info("Starting Silver data quality and transformation checks for inventory.")

            df_checks = df.filter(col("inventory_id").isNotNull())
            df_filter = df_checks.groupby("inventory_id").count().withColumnRenamed("count", "inventory_id_count")
            df_dedup = df_filter.filter(col("inventory_id_count") == 1)
            df_checks = df_checks.join(df_dedup, "inventory_id", "inner")

            df_checks = df_checks.filter(col("warehouse_name").isNotNull())
            df_checks = df_checks.withColumn("warehouse_name", trim("warehouse_name"))
            df_checks = df_checks.filter(col("stock_quantity") >= 0)

            df_checks = df_checks.join(df_product, "product_id", "inner").drop(df_product["is_active"], df_product["last_updated_timestamp"]) 
            df_checks = df_checks.filter(col("reorder_level") >= 0)
            df_checks = df_checks.filter(col("last_stock_update") <= current_date())
            df_checks = df_checks.filter(col("inventory_status").isin("In Stock", "Low Stock", "Out of Stock"))
            df_checks = df_checks.filter(col("last_updated_timestamp").isNotNull())

            df_derived = (
                df_checks
                .withColumn("stock_value", col("stock_quantity") * col("unit_price"))
                .withColumn(
                    "reorder_required",
                    when(col("stock_quantity") <= col("reorder_level"), "Y").otherwise("N")
                )
                .withColumn(
                    "stock_difference",
                    col("stock_quantity") - col("reorder_level")
                )
                .withColumn(
                    "stock_level",
                    when(col("stock_quantity") <= 0, "Critical")
                    .when(col("stock_quantity") <= col("reorder_level"), "Low")
                    .otherwise("Normal")
                )
                .withColumn(
                    "warehouse_region",
                    when(col("warehouse_name").isin("Kolkata", "Patna"), "East")
                    .when(col("warehouse_name").isin("Delhi", "Lucknow"), "North")
                    .when(col("warehouse_name").isin("Mumbai", "Pune"), "West")
                    .when(col("warehouse_name").isin("Chennai", "Bangalore"), "South")
                    .otherwise("Unknown")
                )
            )

            df_inventory_final = df_derived.select(
                col("inventory_id"),
                col("product_id"),
                col("warehouse_name"),
                col("stock_quantity"),
                col("reorder_level"),
                col("last_stock_update"),
                col("inventory_status"),
                col("last_updated_timestamp"),
                col("unit_price"),
                col("inventory_id_count"),
                col("stock_value"),
                col("reorder_required"),
                col("stock_difference"),
                col("stock_level"),
                col("warehouse_region"),
            )

            final_row_count = df_inventory_final.count()
            logger.info(f"Inventory Silver transformation produced {final_row_count} valid rows.")
            df_inventory_final.show(truncate=False)

            today_date = datetime.now().strftime("%Y-%m-%d")
            s3_target_path = f"s3://{Configuration.bucket}/{self.silver_inventory_prefix}/{today_date}"
            logger.info(f"Writing Silver inventory data to: {s3_target_path}")
            df_inventory_final.write.mode("overwrite").parquet(s3_target_path)
            logger.info(f"Silver inventory data successfully written to {s3_target_path}")
            return df_inventory_final
        except Exception as e:
            logger.error(f"Silver inventory data quality or write failed: {e}")
            raise

    def run_inventory_etl(self):
        try:
            logger.info("Starting inventory Silver ETL job.")
            bronze_partitions = self.extract_bronze_table_partition()
            latest_silver_partition = self.extract_silver_table_partition()
            unprocessed_partition = self.bronze_unprocessed_partition(bronze_partitions, latest_silver_partition)

            if not unprocessed_partition:
                logger.info("No new bronze partitions found for inventory data. Silver job skipped.")
                return

            product_reference_df = self.load_product_reference()

            for partition in unprocessed_partition:
                try:
                    s3_path = f"s3://{Configuration.bucket}/{self.bronze_inventory_prefix}/{partition}"
                    logger.info(f"Processing bronze partition {partition} from {s3_path}")
                    validated_df = self.schema_validation(s3_path)
                    self.run_dq_check(validated_df, product_reference_df)
                    logger.info(f"Silver processing completed for partition {partition}")
                except Exception as e:
                    logger.error(f"Failed to process inventory bronze partition {partition}: {e}")
                    raise

            logger.info("Inventory Silver ETL job completed successfully.")
        except Exception as e:
            logger.error(f"Inventory Silver ETL job failed: {e}")
            raise


if __name__ == "__main__":
    inventory = Inventory()
    inventory.run_inventory_etl()
