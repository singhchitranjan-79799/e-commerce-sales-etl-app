import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTERPRISE_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT, ENTERPRISE_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from config import Configuration, SparkConfig
import boto3
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col,
    year,
    month,
    quarter,
    when,
)
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderItem:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()

    order_item_schema = StructType([
        StructField("order_item_id", IntegerType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percentage", DoubleType(), True),
        StructField("line_total", DoubleType(), True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ])

    bronze_order_item_prefix = "bronze_data/order_item_data"
    silver_order_item_prefix = "silver_data/order_item_data"

    def extract_bronze_table_partition(self):
        try:
            s3 = boto3.client("s3")
            bucket = Configuration.bucket
            list_object = s3.list_objects_v2(Bucket=bucket, Prefix=self.bronze_order_item_prefix)

            bronze_partition = set()
            for obj in list_object.get("Contents", []):
                key = obj["Key"]
                if key == "bronze_data/order_item_data/":
                    continue
                folder = key.split("/")[2]
                bronze_partition.add(folder)

            bronze_partition = sorted(bronze_partition)
            logger.info(f"Detected bronze partitions for order item data: {bronze_partition}")
            return bronze_partition
        except Exception as e:
            logger.error(f"Failed to fetch bronze partitions for order item data: {e}")
            raise

    def extract_silver_table_partition(self):
        try:
            s3 = boto3.client("s3")
            unprocessed_partition = s3.list_objects_v2(
                Bucket=Configuration.bucket,
                Prefix=self.silver_order_item_prefix,
            )

            silver_partition = set()
            for object in unprocessed_partition.get("Contents", []):
                key = object["Key"]
                if key == "silver_data/order_item_data/":
                    continue
                part = key.split("/")
                if len(part) < 3 or not part[2]:
                    continue
                silver_partition.add(part[2])

            latest_partition = sorted(silver_partition)[-1] if silver_partition else None
            logger.info(f"Latest silver partition for order item data: {latest_partition}")
            return latest_partition
        except Exception as e:
            logger.error(f"Failed to determine latest silver partition for order item data: {e}")
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
            logger.info(f"Pending bronze partitions to process for order item data: {pending_partition}")
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

            for field in self.order_item_schema:
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
                if field not in self.order_item_schema.fieldNames():
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

    def load_order_reference(self):
        try:
            order_prefix = "silver_data/order_data"
            s3 = boto3.client("s3")
            objects = s3.list_objects_v2(Bucket=Configuration.bucket, Prefix=order_prefix)
            partitions = []
            for obj in objects.get("Contents", []):
                key = obj["Key"]
                if key == "silver_data/order_data/":
                    continue
                part = key.split("/")
                if len(part) >= 3 and part[2]:
                    partitions.append(part[2])

            if not partitions:
                raise ValueError("No silver order partitions found to use as reference data.")

            latest_partition = sorted(partitions)[-1]
            order_path = f"s3://{Configuration.bucket}/{order_prefix}/{latest_partition}"
            logger.info(f"Loading order reference data from: {order_path}")
            df_order = self.start_spark.read.option("inferSchema", "true").parquet(order_path)
            logger.info(f"Order reference loaded successfully with {df_order.count()} rows")
            return df_order
        except Exception as e:
            logger.error(f"Failed to load order reference data: {e}")
            raise

    def run_dq_check(self, df, df_product, df_order):
        try:
            logger.info("Starting Silver data quality and transformation checks for order items.")

            df_checks = df.filter(col("order_item_id").isNotNull())
            df_filter = df_checks.groupby("order_item_id").count().withColumnRenamed("count", "order_item_id_count")
            df_dedup = df_filter.filter(col("order_item_id_count") == 1)
            df_checks = df_checks.join(df_dedup, "order_item_id", "inner")

            df_checks = df_checks.filter(col("quantity") > 0)
            df_checks = df_checks.filter(col("unit_price") > 0)
            df_checks = df_checks.filter(col("line_total") > 0)
            df_checks = df_checks.filter(col("discount_percentage").between(1, 100))
            df_checks = df_checks.filter(col("last_updated_timestamp").isNotNull())

            df_checks = df_checks.join(
                df_product,
                "product_id",
                "inner"
            ).drop(
                df_product["is_active"],
                df_product["last_updated_timestamp"],
                df_product["unit_price"],
            )
            df_checks = df_checks.join(df_order.select("order_id", "order_date"), "order_id", "inner")

            df_derived = (
                df_checks
                .withColumn("gross_amount", col("quantity") * col("unit_price"))
                .withColumn("discount_amount", col("gross_amount") * col("discount_percentage") / 100)
                .withColumn("net_amount", col("gross_amount") - col("discount_amount"))
                .withColumn(
                    "is_discounted",
                    when(col("discount_percentage") > 0, "Y").otherwise("N")
                )
                .withColumn("order_year", year(col("order_date")))
                .withColumn("order_month", month(col("order_date")))
                .withColumn("order_quarter", quarter(col("order_date")))
            )

            df_final = df_derived.select(
                col("order_item_id"),
                col("order_id"),
                col("product_id"),
                col("quantity"),
                col("unit_price"),
                col("discount_percentage"),
                col("line_total"),
                col("last_updated_timestamp"),
                col("gross_amount"),
                col("discount_amount"),
                col("net_amount"),
                col("order_date"),
                col("order_year"),
                col("order_month"),
                col("order_quarter"),
                col("is_discounted"),
            )

            final_row_count = df_final.count()
            logger.info(f"Order item Silver transformation produced {final_row_count} valid rows.")
            df_final.show(truncate=False)

            today_date = datetime.now().strftime("%Y-%m-%d")
            s3_target_path = f"s3://{Configuration.bucket}/{self.silver_order_item_prefix}/{today_date}"
            logger.info(f"Writing Silver order item data to: {s3_target_path}")
            df_final.write.mode("overwrite").parquet(s3_target_path)
            logger.info(f"Silver order item data successfully written to {s3_target_path}")
            return df_final
        except Exception as e:
            logger.error(f"Silver order item data quality or write failed: {e}")
            raise

    def run_order_item_etl(self):
        try:
            logger.info("Starting order item Silver ETL job.")
            bronze_partitions = self.extract_bronze_table_partition()
            latest_silver_partition = self.extract_silver_table_partition()
            unprocessed_partition = self.bronze_unprocessed_partition(bronze_partitions, latest_silver_partition)

            if not unprocessed_partition:
                logger.info("No new bronze partitions found for order item data. Silver job skipped.")
                return

            product_reference_df = self.load_product_reference()
            order_reference_df = self.load_order_reference()

            for partition in unprocessed_partition:
                try:
                    s3_path = f"s3://{Configuration.bucket}/{self.bronze_order_item_prefix}/{partition}"
                    logger.info(f"Processing bronze partition {partition} from {s3_path}")
                    validated_df = self.schema_validation(s3_path)
                    self.run_dq_check(validated_df, product_reference_df, order_reference_df)
                    logger.info(f"Silver processing completed for partition {partition}")
                except Exception as e:
                    logger.error(f"Failed to process order item bronze partition {partition}: {e}")
                    raise

            logger.info("Order item Silver ETL job completed successfully.")
        except Exception as e:
            logger.error(f"Order item Silver ETL job failed: {e}")
            raise


if __name__ == "__main__":
    order_item = OrderItem()
    order_item.run_order_item_etl()
