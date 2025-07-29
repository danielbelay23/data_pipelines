import sys
import os
import asyncio
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.data_jobs.data_ingestion import main_runner
from src.data_jobs.db_manager import main as db_main

async def run_full_pipeline():
    """runs the complete data ingestion and db sync pipeline"""
    await main_runner()
    print("data ingested successfully")
    db_main()
    print("db sync'd successfully")
    print("---full pipeline is done---")

def main():
    """
    adding arguments to allow running individually
    note: default is to run the full pipeline if no specific flag is given

    examples:
        python main.py  ---> run the full pipeline (ingest then sync DB)
        python main.py --ingest-only  ---> run only the data ingestion
        python main.py --db-sync-only  ---> run only the database sync
    """
    parser = argparse.ArgumentParser(
        description="run the twitter data pipeline",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ingest-only",
        action="store_true",
    )
    group.add_argument(
        "--db-sync-only",
        action="store_true",
    )
    args = parser.parse_args()

    if args.ingest_only:
        print("--- running ingestion step only ---")
        asyncio.run(main_runner())
        print("--- ingestion step is done ---")
    elif args.db_sync_only:
        print("--- running db sync step only ---")
        db_main()
        print("--- db sync step is done ---")
    else:
        asyncio.run(run_full_pipeline())

if __name__ == "__main__":
    main()