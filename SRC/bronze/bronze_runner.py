from bronze.bronze_orchestration import bronze_table_jobs


def run_bronze_ingestion():
    return bronze_table_jobs()


def main():
    run_bronze_ingestion()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
