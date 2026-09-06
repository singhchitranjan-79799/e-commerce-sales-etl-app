import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTERPRISE_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT, ENTERPRISE_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from enterprise_pipeline.config import Configuration, SparkConfig
from enterprise_pipeline.connection.connection1 import read_table
import boto3
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col,
    current_date,
    trim,
    year,
    month,
    quarter,
    dayofmonth,
    when,
)
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Order:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()

    order_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("payment_id", IntegerType(), True),
        StructField("status_id", IntegerType(), True),
        StructField("shipping_city", StringType(), True),
        StructField("shipping_state", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("order_total", DoubleType(), True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ])

    bronze_order_prefix = "bronze_data/orderss_data"
    silver_order_prefix = "silver_data/order_data"

    def extract_bronze_table_partition(self):
        try:
            s3 = boto3.client("s3")
            bucket = Configuration.bucket
            list_object = s3.list_objects_v2(Bucket=bucket, Prefix=self.bronze_order_prefix)

            bronze_partition = set()
            for obj in list_object.get("Contents", []):
                key = obj["Key"]
                if key == "bronze_data/orderss_data/":
                    continue
                folder = key.split("/")[2]
                bronze_partition.add(folder)

            bronze_partition = sorted(bronze_partition)
            logger.info(f"Detected bronze partitions for order data: {bronze_partition}")
            return bronze_partition
        except Exception as e:
            logger.error(f"Failed to fetch bronze partitions for order data: {e}")
            raise

    def extract_silver_table_partition(self):
        try:
            s3 = boto3.client("s3")
            unprocessed_partition = s3.list_objects_v2(
                Bucket=Configuration.bucket,
                Prefix=self.silver_order_prefix,
            )

            silver_partition = set()
            for object in unprocessed_partition.get("Contents", []):
                key = object["Key"]
                if key == "silver_data/order_data/":
                    continue
                part = key.split("/")
                if len(part) < 3 or not part[2]:
                    continue
                silver_partition.add(part[2])

            latest_partition = sorted(silver_partition)[-1] if silver_partition else None
            logger.info(f"Latest silver partition for order data: {latest_partition}")
            return latest_partition
        except Exception as e:
            logger.error(f"Failed to determine latest silver partition for order data: {e}")
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
            logger.info(f"Pending bronze partitions to process for order data: {pending_partition}")
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

            for field in self.order_schema:
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
                if field not in self.order_schema.fieldNames():
                    validation_results["extra_field"].append(field)

            logger.info(f"Schema validation result for {s3_path}: {validation_results}")
            if validation_results["missing_field"] or validation_results["extra_field"] or validation_results["nullability_mismatch"]:
                raise ValueError(f"Schema validation failed for {s3_path}: {validation_results}")
            return df
        except Exception as e:
            logger.error(f"Schema validation error for {s3_path}: {e}")
            raise

    def load_customer_reference(self):
        try:
            customer_prefix = "silver_data/customer_data"
            s3 = boto3.client("s3")
            objects = s3.list_objects_v2(Bucket=Configuration.bucket, Prefix=customer_prefix)
            partitions = []
            for obj in objects.get("Contents", []):
                key = obj["Key"]
                if key == "silver_data/customer_data/":
                    continue
                part = key.split("/")
                if len(part) >= 3 and part[2]:
                    partitions.append(part[2])

            if not partitions:
                raise ValueError("No silver customer partitions found to use as reference data.")

            latest_partition = sorted(partitions)[-1]
            customer_path = f"s3://{Configuration.bucket}/{customer_prefix}/{latest_partition}"
            logger.info(f"Loading customer reference data from: {customer_path}")
            df_customer = self.start_spark.read.option("inferSchema", "true").parquet(customer_path)
            logger.info(f"Customer reference loaded successfully with {df_customer.count()} rows")
            return df_customer
        except Exception as e:
            logger.error(f"Failed to load customer reference data: {e}")
            raise

    def load_payment_lookup(self):
        try:
            table = "payment_lookup"
            logger.info(f"Loading payment lookup from MySQL table: {table}")
            lookup_df = read_table(self.start_spark, table, Configuration.mysql_config)
            if lookup_df is None or lookup_df.rdd.isEmpty():
                raise ValueError(f"Payment lookup table '{table}' returned no rows.")
            logger.info(f"Payment lookup loaded successfully with {lookup_df.count()} rows")
            return lookup_df
        except Exception as e:
            logger.error(f"Failed to load payment lookup from MySQL: {e}")
            raise

    def load_status_lookup(self):
        try:
            table = "status_lookup"
            logger.info(f"Loading status lookup from MySQL table: {table}")
            lookup_df = read_table(self.start_spark, table, Configuration.mysql_config)
            if lookup_df is None or lookup_df.rdd.isEmpty():
                raise ValueError(f"Status lookup table '{table}' returned no rows.")
            logger.info(f"Status lookup loaded successfully with {lookup_df.count()} rows")
            return lookup_df
        except Exception as e:
            logger.error(f"Failed to load status lookup from MySQL: {e}")
            raise

    def run_dq_check(self, df, customer_df_final, payment_lookup_df, status_lookup_df):
        try:
            logger.info("Starting Silver data quality and transformation checks for orders.")

            df_checks = df.filter(col("order_id").isNotNull())
            df_filter = df_checks.groupby("order_id").count().withColumnRenamed("count", "order_id_count")
            df_dedup = df_filter.filter(col("order_id_count") == 1)
            df_checks = df_checks.join(df_dedup, "order_id", "inner")

            df_checks = df_checks.join(customer_df_final, "customer_id", "inner").drop(customer_df_final["is_active"], customer_df_final["last_updated_timestamp"])
            df_checks = df_checks.join(payment_lookup_df, "payment_id", "inner").drop(payment_lookup_df["is_active"])
            df_checks = df_checks.join(status_lookup_df, "status_id", "inner").drop(status_lookup_df["is_active"])

            df_checks = df_checks.filter(col("order_date") <= current_date())
            df_checks = df_checks.filter(col("order_total") > 0)
            df_checks = df_checks.filter(col("shipping_city").isNotNull())
            df_checks = df_checks.filter(col("shipping_state").isNotNull())
            df_checks = df_checks.withColumn("shipping_address", trim(col("shipping_address")))
            df_checks = df_checks.filter(col("last_updated_timestamp").isNotNull())

            df_derived = (
                df_checks
                .withColumn("order_year", year(col("order_date")))
                .withColumn("order_month", month(col("order_date")))
                .withColumn("order_quarter", quarter(col("order_date")))
                .withColumn("order_day", dayofmonth(col("order_date")))
                .withColumn(
                    "is_delivered",
                    when(col("status_name") == "Delivered", True).otherwise(False)
                )
            )

            order_df_final = df_derived.select(
                col("order_id"),
                col("customer_id"),
                col("order_date"),
                col("payment_id"),
                col("payment_method"),
                col("status_id"),
                col("status_name"),
                col("shipping_city"),
                col("shipping_state"),
                col("shipping_address"),
                col("order_total"),
                col("last_updated_timestamp"),
                col("order_year"),
                col("order_month"),
                col("order_quarter"),
                col("order_day"),
                col("is_delivered"),
            )

            final_row_count = order_df_final.count()
            logger.info(f"Order Silver transformation produced {final_row_count} valid rows.")
            order_df_final.show(truncate=False)

            today_date = datetime.now().strftime("%Y-%m-%d")
            s3_target_path = f"s3://{Configuration.bucket}/{self.silver_order_prefix}/{today_date}"
            logger.info(f"Writing Silver order data to: {s3_target_path}")
            order_df_final.write.mode("overwrite").parquet(s3_target_path)
            logger.info(f"Silver order data successfully written to {s3_target_path}")
            return order_df_final
        except Exception as e:
            logger.error(f"Silver order data quality or write failed: {e}")
            raise

    def run_order_etl(self):
        try:
            logger.info("Starting order Silver ETL job.")
            bronze_partitions = self.extract_bronze_table_partition()
            latest_silver_partition = self.extract_silver_table_partition()
            unprocessed_partition = self.bronze_unprocessed_partition(bronze_partitions, latest_silver_partition)

            if not unprocessed_partition:
                logger.info("No new bronze partitions found for order data. Silver job skipped.")
                return

            customer_reference_df = self.load_customer_reference()
            payment_lookup_df = self.load_payment_lookup()
            status_lookup_df = self.load_status_lookup()

            for partition in unprocessed_partition:
                try:
                    s3_path = f"s3://{Configuration.bucket}/{self.bronze_order_prefix}/{partition}"
                    logger.info(f"Processing bronze partition {partition} from {s3_path}")
                    validated_df = self.schema_validation(s3_path)
                    self.run_dq_check(validated_df, customer_reference_df, payment_lookup_df, status_lookup_df)
                    logger.info(f"Silver processing completed for partition {partition}")
                except Exception as e:
                    logger.error(f"Failed to process order bronze partition {partition}: {e}")
                    raise

            logger.info("Order Silver ETL job completed successfully.")
        except Exception as e:
            logger.error(f"Order Silver ETL job failed: {e}")
            raise


if __name__ == "__main__":
    order = Order()
    order.run_order_etl()
