import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SparkConfig:
    """Singleton Spark session manager for local Docker runs and AWS Glue runtime."""

    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def create_spark_session(self):
        if self._spark is not None:
            return self._spark

        # AWS Glue runtime provides GlueContext and spark_session automatically.
        try:
            from awsglue.context import GlueContext
            from pyspark.context import SparkContext

            sc = SparkContext.getOrCreate()
            glue_context = GlueContext(sc)
            self._spark = glue_context.spark_session
            logger.info("Glue Spark session created successfully.")
            return self._spark
        except Exception:
            logger.info("Falling back to local Spark session for Docker / non-Glue execution.")
            self._spark = SparkSession.builder.master("local[*]").appName("TransactionProcessing").getOrCreate()
            logger.info("Local Spark session created successfully.")
            return self._spark

    def stop_spark_session(self):
        if self._spark is not None:
            logger.info("Stopping Spark Session")
            self._spark.stop()
            self._spark = None
            logger.info("Spark Session stopped successfully")
