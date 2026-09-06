from Config.config import Configuration
from Config.spark_session import SparkConfig
from connection.mysql_connection import read_table
import boto3
from pyspark.sql.types import *
from pyspark.sql.functions import length,concat,floor,datediff,current_date,lit,when,col
from Config.spark_session import SparkConfig
import logging
from datetime import datetime


logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

class Customer:
    def __init__(self):
            spark_config = SparkConfig()
            self.start_spark=spark_config.create_spark_session()

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
            StructField("is_active", StringType(), True)
        ])
    bronze_customer_prefix="bronze_data/customer_data"
    silver_customer_prefix="silver_data/customer_data"
    def extract_bronze_table_partition(self):
        s3=boto3.client("s3")
        bucket=Configuration.bucket
        
        list_object=s3.list_objects_v2(
                Bucket=bucket,
                Prefix=self.bronze_customer_prefix
            )

        bronze_partition=set()
        for obj in list_object["Contents"]:
            key=obj["Key"]
            if key =='bronze_data/customer_data/':
                continue
            folder=key.split("/")[2]
            bronze_partition.add(folder)
        bronze_partition=sorted(bronze_partition)
        print(bronze_partition)
        return bronze_partition




    def extract_silver_table_partition(self):
        s3=boto3.client("s3")

        

        unprocessed_partition=s3.list_objects_v2(
            Bucket=Configuration.bucket,
            Prefix=self.silver_customer_prefix
        )
        silver_partition=set()
        for object in unprocessed_partition["Contents"]:
            key=object["Key"]
            print(key)
            if key=="silver_data/customer_data/": # we have to skip this the fisrt key as it doesn't have any parition key
                continue
            part=key.split("/")
            print(part)
            if len(part)<3 or not part[2]:
                continue
            partition=part[2]
            silver_partition.add(partition)
        if silver_partition:

            latest_partition=sorted(silver_partition)[-1]

        else:
            latest_partition=None
        print(latest_partition)     
        return latest_partition
           

    def bronze_unprocessed_partition(self,bronze_partition,latest_partition):

        pending_parition=[]
        if not latest_partition:
            pending_parition.extend(bronze_partition)
        else:
            for partition in bronze_partition:
                if partition>latest_partition:
                    pending_parition.append(partition)
        print(pending_parition)            
        return pending_parition       
            
    
    

        

    def schema_validation(self,s3_path:str):

        df=self.start_spark.read.option("inferSchema","true").parquet(s3_path)
        validation_results={
        "missing_field":[],
        "extra_field":[],
        #"datatype_mismatch":[],
        "nullability_mismatch":[]
        }
        for field in self.customer_schema:
            if field.name not in df.columns:
                validation_results["missing_field"].append(field.name)
            else:
                expected_type=field.dataType
                actual_type=df.schema[field.name].dataType
                expected_nullable_value=field.nullable
                actual_nullable_value=df.schema[field.name].nullable
                """if  expected_type!=actual_type:
                    validation_results["datatype_mismatch"].append({
                        "field_name":field.name,
                        "expected_type":expected_type,
                        "actual_type":actual_type

                    })"""
                if expected_nullable_value!=actual_nullable_value:
                    validation_results["nullability_mismatch"].append({
                        "field_name":field.name,
                        "expected_nullable_value":expected_nullable_value,
                        "actual_nullable_value":actual_nullable_value

                    })

        for field in df.columns:
            if field not in self.customer_schema.fieldNames():
                validation_results['extra_field'].append(field)
        print(validation_results)        
        return df



    def run_dq_check(self,df,customer_segment_df):
         
        #DQ Check :(Note :for this please explore some apps like great expectation to expediate this work        )
 
        #primary key validation:
        df_checks=df.filter(col("customer_id").isNotNull())

        #Remove duplicate customer_id
        df_filter=df_checks.groupby("customer_id").count().withColumnRenamed("count", "customer_id_count")
        df_dedup=df_filter.filter(col("customer_id_count")==1)


        df_checks=df_checks.join(df_dedup,"customer_id","inner")

        #Customer segment Validation

        df_checks=df_checks.join(customer_segment_df,"customer_segment_id","inner").drop(customer_segment_df["is_active"])




        #First name validation
        df_checks=df_checks.filter(col("first_name").isNotNull())


        #Last name validation

        df_checks=df_checks.filter(col("last_name").isNotNull())

        #Email deduplication

        df_filter=df_checks.groupby("email").count().withColumnRenamed("count", "email_count")
        df_dedup=df_filter.filter(col("email_count")==1)


        df_checks=df_checks.join(df_dedup,"email","inner")
        #Email validation

        df_checks=df_checks.filter(col("email").isNotNull())


        #Phone validation

        df_checks=df_checks.filter(length(col("phone_number"))==10)

        #derived Column
        df_derived=df_checks.withColumn("full_name",concat("first_name",lit(" "),"last_name"))\
                            .withColumn ("age",floor(datediff(current_date(),(col("dob")))/365.25))\
                            .withColumn(
                                    "age_group",
                                    when((col("age") >= 18) & (col("age") <= 25), "18-25")
                                    .when((col("age") >= 26) & (col("age") <= 40), "26-40")
                                    .when((col("age") >= 41) & (col("age") <= 60), "41-60")
                                    .when(col("age") > 60, "60+")
                                    .otherwise("Invalid")
                                )\
                            .withColumn(
                            "customer_tenure_days",
                            datediff(
                                current_date(),
                                col("registration_date")
                            )
                        )\
                        .withColumn(
                        "customer_status",
                        when(col("is_active") == "Y", "Active")
                        .when(col("is_active") == "N", "Inactive")
                        .otherwise("Unknown")
                        )\
                        .withColumn(
                        "customer_status",
                        when(col("is_active") == "Y", "Active")
                        .when(col("is_active") == "N", "Inactive")
                        .otherwise("Unknown")
                        )\
                        .withColumn(
                        "is_premium_customer",
                        when(col("segment_name") == "Gold", True)
                        .otherwise(False)
                        )



        customer_df_final=df_derived.select(
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
        customer_df_final.show()

      
        today_date=datetime.now().strftime("%Y-%m-%-d")
        s3_target_path=f"s3://e-commerce-sales-etl/silver_data/customer_data/{today_date}"
        customer_df_final.write.mode("overwrite").parquet(s3_target_path)
    

    def load_customer_segment_lookup(self):
        table="customer_segment_lookup"
        customer_segment_df =read_table(self.start_spark,table,Configuration.mysql_config)
        return customer_segment_df

    def run_customer_etl(self):
        #Getting bronze table all unprocessed partition
        extract_bronze_partition=self.extract_bronze_table_partition()

        #Getting silver partition
        extract_silver_partition=self.extract_silver_table_partition()

        #Filtering all remaining umprocessed partition  
        unprocessed_partition=self.bronze_unprocessed_partition(extract_bronze_partition,extract_silver_partition)

        load_customer_segment_table=self.load_customer_segment_lookup()
        #Schema  Validation
        for partition in unprocessed_partition:
        
                s3_path=f"s3://{Configuration.bucket}/{self.bronze_customer_prefix}/{partition}" 
                Schema_validation=self.schema_validation(s3_path)

                #Checking  data quality 

                data_quality=self.run_dq_check(Schema_validation,load_customer_segment_table)
customer=Customer()
run_customer=customer.run_customer_etl()              



       
        



        
         
       



