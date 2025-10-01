"""Sales Demo Pipeline Orchestrator

Main execution pipeline for the sales demo. This demo is a reference implementation of the sales demo.

Execution:
    python -m examples.sales_demo.main
    python -m examples.sales_demo.main --schema my_custom_schema
"""

import sys
import os
import json
from pathlib import Path

from config import get_config
from core import *

from .datasets import generate_datamodel

def main():
    # Parse command line arguments using centralized parsing
    cli_overrides = parse_demo_args("Sales Demo Pipeline")
    
    # Load configuration with CLI overrides
    config = get_config(cli_overrides=cli_overrides)
    
    # Setup centralized logging using configured level
    setup_logging(level=config.logging.level, include_timestamp=True, include_module=True)
    logger = get_logger(__name__)
    
    try:
        logger.info("Starting sales demo pipeline...")
        logger.info("This demo creates user profiles and sales transactions with proper relationships")
        
        if cli_overrides:
            logger.info(f"CLI overrides provided: {cli_overrides}")
        logger.info(f"Loaded configuration for environment: {config.app.name}")
        
        # Create data directory structure relative to this script's folder
        script_dir = Path(__file__).parent
        base_data_dir = script_dir / "data"
        base_data_dir.mkdir(exist_ok=True)
        logger.info(f"Created base data directory: {base_data_dir}")
        
        # Generate synthetic datasets
        logger.info("Generating synthetic sales datasets...")
        num_records = cli_overrides.get('records') if cli_overrides else None
        data_model = generate_datamodel(config, num_records)
        
        # Log dataset statistics
        logger.info(f"Generated {len(data_model.datasets)} datasets:")
        for dataset in data_model.datasets:
            logger.info(f"  - {dataset.name}: {len(dataset.data):,} records")
        
        # Save datasets locally to data subfolders
        logger.info("Saving datasets to local data subfolders...")
        saved_paths = []
        
        for dataset in data_model.datasets:
            # Create subfolder for each dataset
            dataset_dir = base_data_dir / dataset.name
            dataset_dir.mkdir(exist_ok=True)
            
            # Save as CSV only
            csv_path = dataset_dir / f"{dataset.name}.csv"
            dataset.data.to_csv(csv_path, index=False)
            
            saved_paths.append(str(csv_path))
            logger.info(f"Saved {dataset.name} to {csv_path}")
        
        logger.info(f"Saved {len(data_model.datasets)} datasets to {len(saved_paths)} files")

        
        # Log summary statistics
        logger.info("\n=== Pipeline Summary ===")
        logger.info(f"Total Users: {len(data_model.get_dataset('user_profiles').data):,}")
        logger.info(f"Total Transactions: {len(data_model.get_dataset('product_sales').data):,}")
        
        logger.info("\nSales demo pipeline completed successfully!")
        logger.info(f"\n📁 Raw data saved to: {base_data_dir.absolute()}")
        
        logger.info("\nGenerated datasets:")
        for dataset in data_model.datasets:
            dataset_dir = base_data_dir / dataset.name
            logger.info(f"  - {dataset.name}: {dataset_dir / f'{dataset.name}.csv'}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.exception("Full error details:")
        sys.exit(1)

if __name__ == "__main__":
    main()