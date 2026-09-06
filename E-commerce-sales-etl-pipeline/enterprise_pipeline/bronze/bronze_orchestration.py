from enterprise_pipeline.bronze.bronze1 import BronzeExtractor
from enterprise_pipeline.config import Configuration


def bronze_table_jobs():
    extractor = BronzeExtractor()

    for table in Configuration.table_name:
        source_table_name = table["source_table"]
        target_table = table["target_table"]

        if table["source_type"] == "my_sql":
            bronze_path = f"{Configuration.bronze_path}/{target_table}/{Configuration.timestamp}"
            extractor.ingest_data_from_mysql_database(source_table_name, bronze_path, source_table_name)
        elif table["source_type"] == "s3":
            s3_path = f"{Configuration.base_path}/{source_table_name}"
            bronze_path = f"{Configuration.bronze_path}/{target_table}/{Configuration.timestamp}"
            extractor.ingest_data_from_s3(source_table_name, s3_path, bronze_path, source_table_name)

    return True
