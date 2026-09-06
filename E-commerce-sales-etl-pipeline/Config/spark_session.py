from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger=logging.getLogger(__name__)

class SparkConfig:
    '''manages spark session and configuration'''

    _instance=None
    _spark=None
    def __new__(cls):
        if cls._instance is None:
            cls._instance=super().__new__(cls)
        return cls._instance   

    def create_spark_session(self):
        if self._spark is None:
            logger.info("Creating new_Spark Session")
            self._spark = SparkSession.builder.master("local[*]").appName("TransactionProcessing").getOrCreate()
            logger.info("Spark Session created successfully")
        return self._spark

    def stop_spark_session(self):
        print(self._spark)
        if self._spark is not None:
            logger.info("Stopping Spark Session")
            self._spark.stop()
            self._spark=None
            logger.info("Spark Session stopped successfully")