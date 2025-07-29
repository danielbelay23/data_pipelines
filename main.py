import sys
import os
import asyncio
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.data_jobs.data_ingestion import main_runner
from src.data_jobs.db_manager import main as db_main

async def run_full_pipeline():
    """Runs the complete data ingestion and database synchronization pipeline."""
    await main_runner()
    print("Data collection completed.")
    db_main()
    print("Database synchronization completed.")
    print("--- Full Pipeline Finished ---")

def main():
    """
    adding arguments to allow running individually

    Examples:
        python main.py                # Run the full pipeline (ingest then sync DB)
        python main.py --ingest-only  # Run only the data ingestion
        python main.py --db-sync-only      # Run only the database sync
    """
    parser = argparse.ArgumentParser(
        description="Run the Twitter data pipeline.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ingest-only",
        action="store_true",
        help="Run only the data ingestion step."
    )
    group.add_argument(
        "--db-sync-only",
        action="store_true",
        help="Run only the database synchronization step."
    )
    args = parser.parse_args()

    if args.ingest_only:
        print("--- Running Ingestion Step Only ---")
        asyncio.run(main_runner())
        print("--- Ingestion Finished ---")
    elif args.db_sync_only:
        print("--- Running Database Sync Step Only ---")
        db_main()
        print("--- Database Sync Finished ---")
    else:
        # Default is torun the full pipeline if no specific flag is given
        asyncio.run(run_full_pipeline())

if __name__ == "__main__":
    main()