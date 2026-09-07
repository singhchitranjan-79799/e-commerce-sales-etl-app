import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENTERPRISE_ROOT = Path(__file__).resolve().parents[1]
for root in (PROJECT_ROOT, ENTERPRISE_ROOT):
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

from silver.customer1 import Customer
from silver.product1 import Product
from silver.inventory1 import Inventory
from silver.order1 import Order
from silver.order_item1 import OrderItem

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("silver_pipeline")


def run_stage(stage_name: str, stage_callable):
    logger.info("Starting Silver stage: %s", stage_name)
    try:
        stage_callable()
        logger.info("Silver stage completed successfully: %s", stage_name)
    except Exception:
        logger.exception("Silver stage failed and the chain will stop: %s", stage_name)
        raise


def execute_silver_chain():
    logger.info("Starting full Silver ETL dependency chain.")

    stages = [
        ("customer", lambda: Customer().run_customer_etl()),
        ("product", lambda: Product().run_product_etl()),
        ("inventory", lambda: Inventory().run_inventory_etl()),
        ("order", lambda: Order().run_order_etl()),
        ("order_item", lambda: OrderItem().run_order_item_etl()),
    ]

    for stage_name, stage_callable in stages:
        run_stage(stage_name, stage_callable)

    logger.info("All Silver ETL stages completed successfully.")
    return True


def main():
    try:
        execute_silver_chain()
        logger.info("Silver pipeline finished successfully.")
        return 0
    except Exception as exc:
        logger.exception("Silver pipeline failed with critical error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
