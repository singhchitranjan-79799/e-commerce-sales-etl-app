import os
from datetime import datetime
from dotenv import load_dotenv

class Configuration:

    #define local path root directory and load env variables
    base_dir=os.path.dirname((os.path.dirname(os.path.abspath(__file__))))
    env_path=os.path.join(base_dir,"Env_variable","credentials.env")
    print(base_dir)
    print(env_path)
    print(base_dir)
    print(os.path.exists(env_path))
    load_dotenv(env_path)

    mysql_config={
            
            "host": os.getenv("host", "host.docker.internal"),
            "port": os.getenv("port", "3306"),
            "database": os.getenv("database", "abc"),
            "username": os.getenv("username","abc"),
            "password": os.getenv("password","abc"),
            "driver": os.getenv("driver", "com.mysql.cj.jdbc.Driver")
        }


    bucket=os.getenv("s3_bucket")
    print(bucket)
    prefix="ingestion_data"
    base_path=f"s3://{bucket}/{prefix}"
    bronze_prefix="bronze_data"
    bronze_path=f"s3://{bucket}/{bronze_prefix}"
    timestamp=datetime.now().strftime("%Y%m%d")


    table_name=[{
                "source_type":"my_sql",
                "source_table":"customers",
                "target_table":"customer_data"
                },
                {
                "source_type":"my_sql",
                "source_table":"products",
                "target_table":"product_data"
                },
                {"source_type":"s3",
                "source_table":"inventory_data",
                "target_table":"inventory_data"
                },
                {"source_type":"s3",
                "source_table":"order_item_data",
                "target_table":"order_item_data"
                    },
                {"source_type":"s3",
                "source_table":"orders_data",
                "target_table":"orderss_data"
                }
                ]