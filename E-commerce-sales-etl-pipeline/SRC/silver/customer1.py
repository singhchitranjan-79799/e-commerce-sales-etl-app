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
from pyspark.sql.functions import length, concat, floor, datediff, current_date, lit, when, col
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Customer:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark = spark_config.create_spark_session()

    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("customer_segment_id", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("last_updated_timestamp", TimestampType(), True),
        StructField("is_active", StringType(), True),
    ])

    bronze_customer_prefix = "bronze_data/customer_data"
    silver_customer_prefix = "silver_data/customer_data"

    def extract_bronze_table_partition(self):
        try:
            s3 = boto3.client("s3")
            bucket = Configuration.bucket
            list_object = s3.list_objects_v2(Bucket=bucket, Prefix=self.bronze_customer_prefix)

            bronze_partition = set()
            for obj in list_object.get("Contents", []):
                key = obj["Key"]
                if key == 'bronze_data/customer_data/':
                    continue
                folder = key.split("/")[2]
                bronze_partition.add(folder)

            bronze_partition = sorted(bronze_partition)
            logger.info(f"Detected bronze partitions for customer data: {bronze_partition}")
            return bronze_partition
        except Exception as e:
            logger.error(f"Failed to fetch bronze partitions for customer data: {e}")
            raise

    def extract_silver_table_partition(self):
        try:
            s3 = boto3.client("s3")
            unprocessed_partition = s3.list_objects_v2(
                Bucket=Configuration.bucket,
                Prefix=self.silver_customer_prefix,
            )
            silver_partition = set()
            for object in unprocessed_partition.get("Contents", []):
                key = object["Key"]
                if key == "silver_data/customer_data/":
                    continue
                part = key.split("/")
                if len(part) < 3 or not part[2]:
                    continue
                partition = part[2]
                silver_partition.add(partition)

            latest_partition = sorted(silver_partition)[-1] if silver_partition else None
            logger.info(f"Latest silver partition for customer data: {latest_partition}")
            return latest_partition
        except Exception as e:
            logger.error(f"Failed to determine latest silver partition for customer data: {e}")
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
            logger.info(f"Pending bronze partitions to process for customer data: {pending_partition}")
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
            for field in self.customer_schema:
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
                if field not in self.customer_schema.fieldNames():
                    validation_results['extra_field'].append(field)

            logger.info(f"Schema validation result for {s3_path}: {validation_results}")
            if validation_results["missing_field"] or validation_results["extra_field"] or validation_results["nullability_mismatch"]:
                raise ValueError(f"Schema validation failed for {s3_path}: {validation_results}")
            return df
        except Exception as e:
            logger.error(f"Schema validation error for {s3_path}: {e}")
            raise

    def run_dq_check(self, df, customer_segment_df):
        try:
            logger.info("Starting Silver data quality and transformation checks for customers.")
            df_checks = df.filter(col("customer_id").isNotNull())

            df_filter = df_checks.groupby("customer_id").count().withColumnRenamed("count", "customer_id_count")
            df_dedup = df_filter.filter(col("customer_id_count") == 1)
            df_checks = df_checks.join(df_dedup, "customer_id", "inner")

            df_checks = df_checks.join(customer_segment_df, "customer_segment_id", "inner").drop(customer_segment_df["is_active"])
            df_checks = df_checks.filter(col("first_name").isNotNull())
            df_checks = df_checks.filter(col("last_name").isNotNull())

            df_filter = df_checks.groupby("email").count().withColumnRenamed("count", "email_count")
            df_dedup = df_filter.filter(col("email_count") == 1)
            df_checks = df_checks.join(df_dedup, "email", "inner")
            df_checks = df_checks.filter(col("email").isNotNull())
            df_checks = df_checks.filter(length(col("phone_number")) == 10)

            df_derived = df_checks.withColumn("full_name", concat("first_name", lit(" "), "last_name")) \
                .withColumn("age", floor(datediff(current_date(), (col("dob"))) / 365.25)) \
                .withColumn(
                    "age_group",
                    when((col("age") >= 18) & (col("age") <= 25), "18-25")
                    .when((col("age") >= 26) & (col("age") <= 40), "26-40")
                    .when((col("age") >= 41) & (col("age") <= 60), "41-60")
                    .when(col("age") > 60, "60+")
                    .otherwise("Invalid")
                ) \
                .withColumn(
                    "customer_tenure_days",
                    datediff(current_date(), col("registration_date"))
                ) \
                .withColumn(
                    "customer_status",
                    when(col("is_active") == "Y", "Active")
                    .when(col("is_active") == "N", "Inactive")
                    .otherwise("Unknown")
                ) \
                .withColumn(
                    "is_premium_customer",
                    when(col("segment_name") == "Gold", True)
                    .otherwise(False)
                )

            customer_df_final = df_derived.select(
                col("email"),
                col("customer_segment_id"),
                col("customer_id"),
                col("first_name"),
                col("last_name"),
                col("phone_number"),
                col("gender"),
                col("dob"),
                col("city"),
                col("state"),
                col("country"),
                col("registration_date"),
                col("last_updated_timestamp"),
                col("is_active"),
                col("customer_id_count"),
                col("segment_name"),
                col("discount_percentage"),
                col("loyalty_points_multiplier"),
                col("description"),
                col("email_count"),
                col("full_name"),
                col("age"),
                col("age_group"),
                col("customer_tenure_days"),
                col("customer_status"),
                col("is_premium_customer")
            )

            final_row_count = customer_df_final.count()
            logger.info(f"Customer Silver transformation produced {final_row_count} valid rows.")
            customer_df_final.show(truncate=False)

            today_date = datetime.now().strftime("%Y-%m-%d")
            s3_target_path = f"s3://{Configuration.bucket}/silver_data/customer_data/{today_date}"
            logger.info(f"Writing Silver customer data to: {s3_target_path}")
            customer_df_final.write.mode("overwrite").parquet(s3_target_path)
            logger.info(f"Silver customer data successfully written to {s3_target_path}")
            return customer_df_final
        except Exception as e:
            logger.error(f"Silver customer data quality or write failed: {e}")
            raise

    def load_customer_segment_lookup(self):
        try:
            table = "customer_segment_lookup"
            logger.info(f"Loading customer segment lookup from MySQL table: {table}")
            customer_segment_df = read_table(self.start_spark, table, Configuration.mysql_config)
            if customer_segment_df is None or customer_segment_df.rdd.isEmpty():
                raise ValueError(f"Customer segment lookup table '{table}' returned no rows.")
            logger.info(f"Customer segment lookup loaded successfully with {customer_segment_df.count()} rows")
            return customer_segment_df
        except Exception as e:
            logger.error(f"Failed to load customer segment lookup from MySQL: {e}")
            raise

    def run_customer_etl(self):
        try:
            logger.info("Starting customer Silver ETL job.")
            extract_bronze_partition = self.extract_bronze_table_partition()
            extract_silver_partition = self.extract_silver_table_partition()
            unprocessed_partition = self.bronze_unprocessed_partition(extract_bronze_partition, extract_silver_partition)

            if not unprocessed_partition:
                logger.info("No new bronze partitions found for customer data. Silver job skipped.")
                return

            load_customer_segment_table = self.load_customer_segment_lookup()
            for partition in unprocessed_partition:
                try:
                    s3_path = f"s3://{Configuration.bucket}/{self.bronze_customer_prefix}/{partition}"
                    logger.info(f"Processing bronze partition {partition} from {s3_path}")
                    schema_validation = self.schema_validation(s3_path)
                    self.run_dq_check(schema_validation, load_customer_segment_table)
                    logger.info(f"Silver processing completed for partition {partition}")
                except Exception as e:
                    logger.error(f"Failed to process customer bronze partition {partition}: {e}")
                    raise

            logger.info("Customer Silver ETL job completed successfully.")
        except Exception as e:
            logger.error(f"Customer Silver ETL job failed: {e}")
            raise


if __name__ == "__main__":
    customer = Customer()
    customer.run_customer_etl()
