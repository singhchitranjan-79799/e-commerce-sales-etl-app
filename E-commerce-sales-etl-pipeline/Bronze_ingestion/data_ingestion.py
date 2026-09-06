from Config.config import Configuration
from Config.spark_session import SparkConfig
from connection.mysql_connection import read_table
from pyspark.sql import DataFrame
from pyspark.sql.functions import col,max
import logging
import boto3 

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

class DataReader:
    def __init__(self):
        spark_config = SparkConfig()
        self.start_spark=spark_config.create_spark_session()
        

    def ingest_data_from_s3(self,table: str,s3_path:str, s3_target_path: str) -> DataFrame:
        logger.info(f"starting reading s3 {table} data")
        try:
            df = self.start_spark.read.option("multiline", "true").json(s3_path)

            logger.info(f"Total data count:{table}-{df.count()}")

            dynamodb = boto3.resource("dynamodb")
            control_table = dynamodb.Table("ecommerce_pipeline_control")
            response = control_table.get_item(Key={"pipeline_table": "orders"})
            

            last_successful_watermark = response.get("Item", {}).get("last_successful_watermark")
            logger.info(f"last data load timetamp:{last_successful_watermark}")

            #Incremental startegy:
            incremental_data_df=df.filter(col("last_updated_timestamp")>(last_successful_watermark))
            new_data_count = incremental_data_df.count()

            logger.info(f"Total incremenatl data count:{table}-{new_data_count}")
            try:
                if new_data_count>0:
                #Find maximaum processed_timestamp
                    max_load_timestamp=incremental_data_df.agg(max(col("last_updated_timestamp")).alias("max_load_timestamp")).collect()[0]["max_load_timestamp"]

                    #Loading the  data to target
                    incremental_data_df.write.mode("overwrite").parquet(s3_target_path)

                    logger.info(f"Data is loaded successfully to {s3_target_path}:Total_data_loaded:{new_data_count}")

                    #Audit information
                    from datetime import datetime
                    last_run_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    #Updating dynomodb paramters
                    record_processed=new_data_count
                    update_watermark=max_load_timestamp
                    control_table.update_item(Key={"pipeline_table":"orders"},
                                    UpdateExpression="""
                                                    SET last_successful_watermark = :wm,
                                                        last_run_status = :status,
                                                        last_run_timestamp = :run_time,
                                                        records_processed = :count
                                                    """,
                                    ExpressionAttributeValues={
                                            ":wm": update_watermark,
                                            ":status": "SUCCESS",
                                            ":run_time": last_run_timestamp,
                                            ":count": record_processed
                                    }
                            )
                    logger.info(f"dynomo db parameters has been updated successfully with:{update_watermark},{last_run_timestamp},{record_processed}")    
                else:
                    logger.info("no incremental data found")
            except Exception as e:
                logging.error(f"error found loading the data:{table}-{e}")
            return incremental_data_df                 
        except Exception as e:
            logger.error(f"error found while loading the incremental_data :{table},{e}") 
            raise


    def ingest_data_from_mysql_database(self,table:str,s3_target_path:str) ->DataFrame:
         
        logger.info(f"starting reading my_sql {table} data")
        try:
            df =read_table(self.start_spark,table,Configuration.mysql_config)

            logger.info(f"Total data count:{table}-{df.count()}")

            dynamodb = boto3.resource("dynamodb")
            control_table = dynamodb.Table("ecommerce_pipeline_control")
            response = control_table.get_item(Key={"pipeline_table": "orders"})
            

            last_successful_watermark = response.get("Item", {}).get("last_successful_watermark")
            logger.info(f"last data load timetamp:{last_successful_watermark}")

            #Incremental startegy:
            incremental_data_df=df.filter(col("last_updated_timestamp")>(last_successful_watermark))
            new_data_count = incremental_data_df.count()

            logger.info(f"Total incremenatl data count:{table}-{new_data_count}")
            try:
                if new_data_count>0:
                #Find maximaum processed_timestamp
                    max_load_timestamp=incremental_data_df.agg(max(col("last_updated_timestamp")).alias("max_load_timestamp")).collect()[0]["max_load_timestamp"]

                    #Loading the  data to target
                    incremental_data_df.write.mode("overwrite").parquet(s3_target_path)

                    logger.info(f"Data is loaded successfully to {s3_target_path}:Total_data_loaded:{new_data_count}")

                    #Audit information
                    from datetime import datetime
                    last_run_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    #Updating dynomodb paramters
                    record_processed=new_data_count
                    update_watermark=max_load_timestamp
                    control_table.update_item(Key={"pipeline_table":"orders"},
                                    UpdateExpression="""
                                                    SET last_successful_watermark = :wm,
                                                        last_run_status = :status,
                                                        last_run_timestamp = :run_time,
                                                        records_processed = :count
                                                    """,
                                    ExpressionAttributeValues={
                                            ":wm": update_watermark,
                                            ":status": "SUCCESS",
                                            ":run_time": last_run_timestamp,
                                            ":count": record_processed
                                    }
                            )
                    logger.info(f"dynomo db parameters has been updated successfully with:{update_watermark},{last_run_timestamp},{record_processed}")    
                else:
                    logger.info("no incremental data found")
            except Exception as e:
                logging.error(f"error found loading the data:{table}-{e}")
            return incremental_data_df                 
        except Exception as e:
            logger.error(f"error found while loading the incremental_data :{table},{e}") 
            raise

               

if __name__=="__main__":
    reader=DataReader()
    for table in Configuration.table_name:
            if table["source_type"]=="my_sql":
                source_table_name=table["source_table"]
                target_table=table["target_table"]

                s3_bronze_path=f"{Configuration.bronze_path}/{target_table}/{Configuration.timestamp}"
                load_mysql_data=reader.ingest_data_from_mysql_database(source_table_name,s3_bronze_path) 

                # print(load_mysql_data)
            elif table["source_type"]=="s3":
                source_table_name=table["source_table"]
                target_table=table["target_table"]

                s3_path=f"{Configuration.base_path}/{table['source_table']}"
                s3_bronze_path=f"{Configuration.bronze_path}/{target_table}/{Configuration.timestamp}"

                
                load_s3_data=reader.ingest_data_from_s3(source_table_name,s3_path,s3_bronze_path)
                print(load_s3_data)

                



