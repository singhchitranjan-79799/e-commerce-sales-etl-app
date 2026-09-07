"""MySQL JDBC helpers for Spark reads in the enterprise ETL app."""

from typing import Dict

from config import Configuration
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_jdbc_url(mysql_config: Dict[str, str] | None = None) -> str:
    if mysql_config is None:
        mysql_config = Configuration.mysql_config
    return f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"


def get_connection_properties(mysql_config: Dict[str, str] | None = None) -> Dict[str, str]:
    if mysql_config is None:
        mysql_config = Configuration.mysql_config
    return {
        "user": mysql_config.get("username"),
        "password": mysql_config.get("password"),
        "driver": mysql_config.get("driver", "com.mysql.cj.jdbc.Driver"),
    }


def read_table(spark, table: str, mysql_config: Dict[str, str] | None = None, fetchsize: int = 10000):
    """Read a MySQL table into a Spark DataFrame using JDBC."""
    url = get_jdbc_url(mysql_config)
    props = get_connection_properties(mysql_config)
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("driver", props["driver"])
            .option("user", props["user"])
            .option("password", props["password"])
            .option("fetchsize", str(fetchsize))
            .load()
        )
        return df

    except Exception as e:
        logger.error(f"error while loading the data from {table},{e}")
        raise
