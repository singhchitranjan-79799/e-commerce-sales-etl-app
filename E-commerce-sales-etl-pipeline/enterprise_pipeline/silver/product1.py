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
    datediff,
    trim,
    when,
)
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Product:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()

    product_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("manufacturing_date", DateType(), True),
        StructField("expiry_date", DateType(), True),
        StructField("weight_grams", DecimalType(8, 2), True),
        StructField("color", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("last_updated_timestamp", TimestampType(), True),
    ])

    bronze_product_prefix = "bronze_data/product_data"
    silver_product_prefix = "silver_data/product_data"

    def extract_bronze_table_partition(self):
        try:
            s3 = boto3.client("s3")
            bucket = Configuration.bucket
            list_object = s3.list_objects_v2(Bucket=bucket, Prefix=self.bronze_product_prefix)

            bronze_partition = set()
            for obj in list_object.get("Contents", []):
                key = obj["Key"]
                if key == "bronze_data/product_data/":
                    continue
                folder = key.split("/")[2]
                bronze_partition.add(folder)

            bronze_partition = sorted(bronze_partition)
            logger.info(f"Detected bronze partitions for product data: {bronze_partition}")
            return bronze_partition
        except Exception as e:
            logger.error(f"Failed to fetch bronze partitions for product data: {e}")
            raise

    def extract_silver_table_partition(self):
        try:
            s3 = boto3.client("s3")
            unprocessed_partition = s3.list_objects_v2(
                Bucket=Configuration.bucket,
                Prefix=self.silver_product_prefix,
            )

            silver_partition = set()
            for object in unprocessed_partition.get("Contents", []):
                key = object["Key"]
                if key == "silver_data/product_data/":
                    continue
                part = key.split("/")
                if len(part) < 3 or not part[2]:
                    continue
                silver_partition.add(part[2])

            latest_partition = sorted(silver_partition)[-1] if silver_partition else None
            logger.info(f"Latest silver partition for product data: {latest_partition}")
            return latest_partition
        except Exception as e:
            logger.error(f"Failed to determine latest silver partition for product data: {e}")
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
            logger.info(f"Pending bronze partitions to process for product data: {pending_partition}")
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

            for field in self.product_schema:
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
                if field not in self.product_schema.fieldNames():
                    validation_results["extra_field"].append(field)

            logger.info(f"Schema validation result for {s3_path}: {validation_results}")
            if validation_results["missing_field"] or validation_results["extra_field"] or validation_results["nullability_mismatch"]:
                raise ValueError(f"Schema validation failed for {s3_path}: {validation_results}")
            return df
        except Exception as e:
            logger.error(f"Schema validation error for {s3_path}: {e}")
            raise

    def load_category_lookup(self):
        try:
            table = "category_lookup"
            logger.info(f"Loading category lookup from MySQL table: {table}")
            category_lookup_df = read_table(self.start_spark, table, Configuration.mysql_config)
            if category_lookup_df is None or category_lookup_df.rdd.isEmpty():
                raise ValueError(f"Category lookup table '{table}' returned no rows.")
            logger.info(f"Category lookup loaded successfully with {category_lookup_df.count()} rows")
            return category_lookup_df
        except Exception as e:
            logger.error(f"Failed to load category lookup from MySQL: {e}")
            raise

    def run_dq_check(self, df, category_lookup_df):
        try:
            logger.info("Starting Silver data quality and transformation checks for products.")

            df_checks = df.filter(col("product_id").isNotNull())
            df_filter = df_checks.groupby("product_id").count().withColumnRenamed("count", "product_id_count")
            df_dedup = df_filter.filter(col("product_id_count") == 1)
            df_checks = df_checks.join(df_dedup, "product_id", "inner")

            df_checks = df_checks.filter(col("product_name").isNotNull())
            df_checks = df_checks.withColumn("product_name", trim("product_name"))
            df_checks = df_checks.filter(col("brand").isNotNull())

            df_checks = df_checks.join(category_lookup_df, "category_id", "inner").drop(category_lookup_df["is_active"])
            df_checks = df_checks.filter((col("unit_price").isNotNull()) & (col("unit_price") > 0))
            df_checks = df_checks.filter(
                (col("manufacturing_date").isNotNull()) &
                (col("expiry_date").isNotNull()) &
                (col("expiry_date") > col("manufacturing_date"))
            )
            df_checks = df_checks.filter(col("weight_grams") > 0)
            df_checks = df_checks.withColumn("color", trim(col("color")))
            df_checks = df_checks.filter(col("last_updated_timestamp").isNotNull())

            df_derived = (
                df_checks
                .withColumn("product_age_days", datediff(current_date(), col("manufacturing_date")))
                .withColumn(
                    "price_range",
                    when(col("unit_price") < 500, "Budget")
                    .when((col("unit_price") >= 500) & (col("unit_price") <= 2000), "Mid Range")
                    .when(col("unit_price") > 2000, "Premium")
                    .otherwise("Unknown")
                )
                .withColumn(
                    "product_status",
                    when(col("is_active") == "Y", "Active")
                    .when(col("is_active") == "N", "Inactive")
                    .otherwise("Unknown")
                )
            )

            df_product = df_derived.select(
                col("product_id"),
                col("product_name"),
                col("brand"),
                col("category_id"),
                col("unit_price"),
                col("manufacturing_date"),
                col("expiry_date"),
                col("weight_grams"),
                col("color"),
                col("is_active"),
                col("last_updated_timestamp"),
                col("product_age_days"),
                col("price_range"),
                col("product_status"),
                col("category_name"),
            )

            final_row_count = df_product.count()
            logger.info(f"Product Silver transformation produced {final_row_count} valid rows.")
            df_product.show(truncate=False)

            today_date = datetime.now().strftime("%Y-%m-%d")
            s3_target_path = f"s3://{Configuration.bucket}/{self.silver_product_prefix}/{today_date}"
            logger.info(f"Writing Silver product data to: {s3_target_path}")
            df_product.write.mode("overwrite").parquet(s3_target_path)
            logger.info(f"Silver product data successfully written to {s3_target_path}")
            return df_product
        except Exception as e:
            logger.error(f"Silver product data quality or write failed: {e}")
            raise

    def run_product_etl(self):
        try:
            logger.info("Starting product Silver ETL job.")
            bronze_partitions = self.extract_bronze_table_partition()
            latest_silver_partition = self.extract_silver_table_partition()
            unprocessed_partition = self.bronze_unprocessed_partition(bronze_partitions, latest_silver_partition)

            if not unprocessed_partition:
                logger.info("No new bronze partitions found for product data. Silver job skipped.")
                return

            category_lookup_df = self.load_category_lookup()

            for partition in unprocessed_partition:
                try:
                    s3_path = f"s3://{Configuration.bucket}/{self.bronze_product_prefix}/{partition}"
                    logger.info(f"Processing bronze partition {partition} from {s3_path}")
                    validated_df = self.schema_validation(s3_path)
                    self.run_dq_check(validated_df, category_lookup_df)
                    logger.info(f"Silver processing completed for partition {partition}")
                except Exception as e:
                    logger.error(f"Failed to process product bronze partition {partition}: {e}")
                    raise

            logger.info("Product Silver ETL job completed successfully.")
        except Exception as e:
            logger.error(f"Product Silver ETL job failed: {e}")
            raise


if __name__ == "__main__":
    product = Product()
    product.run_product_etl()
