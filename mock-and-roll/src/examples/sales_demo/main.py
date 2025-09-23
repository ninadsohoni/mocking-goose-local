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
        
        # Create data directory structure
        base_data_dir = Path("data")
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
        
        # Create SQL queries for Silver and Gold layers
        logger.info("Creating SQL queries for Silver and Gold layers...")
        
        sql_queries = {
            "description": "SQL queries for creating Silver and Gold layer tables from raw sales demo data",
            "bronze_tables": {
                "user_profiles": "Raw customer dimension data",
                "product_sales": "Raw sales fact data"
            },
            "silver_queries": {
                "daily_sales_silver": {
                    "description": "Daily sales aggregations",
                    "query": """
                        SELECT 
                            DATE(sale_date) as date,
                            COUNT(DISTINCT transaction_id) as num_transactions,
                            COUNT(DISTINCT user_id) as unique_customers,
                            SUM(amount) as total_sales,
                            AVG(amount) as avg_transaction_value,
                            SUM(quantity) as units_sold
                        FROM product_sales
                        GROUP BY DATE(sale_date)
                        ORDER BY date DESC
                    """.strip()
                },
                "customer_lifetime_value_silver": {
                    "description": "Customer lifetime value analysis",
                    "query": """
                        SELECT 
                            c.user_id,
                            c.full_name,
                            c.email,
                            c.customer_type,
                            c.signup_date,
                            COUNT(DISTINCT s.transaction_id) as total_transactions,
                            COALESCE(SUM(s.amount), 0) as lifetime_value,
                            COALESCE(AVG(s.amount), 0) as avg_order_value,
                            MAX(s.sale_date) as last_purchase_date,
                            DATEDIFF(CURRENT_DATE(), c.signup_date) as days_since_signup
                        FROM user_profiles c
                        LEFT JOIN product_sales s ON c.user_id = s.user_id
                        GROUP BY c.user_id, c.full_name, c.email, c.customer_type, c.signup_date
                    """.strip()
                },
                "product_performance_silver": {
                    "description": "Product performance metrics",
                    "query": """
                        SELECT 
                            product,
                            COUNT(DISTINCT transaction_id) as num_transactions,
                            SUM(quantity) as units_sold,
                            SUM(amount) as revenue,
                            AVG(unit_price) as avg_price,
                            COUNT(DISTINCT user_id) as unique_customers
                        FROM product_sales
                        GROUP BY product
                        ORDER BY revenue DESC
                    """.strip()
                },
                "customer_segments_silver": {
                    "description": "Customer segmentation analysis",
                    "query": """
                        WITH customer_metrics AS (
                            SELECT 
                                user_id,
                                COUNT(DISTINCT transaction_id) as transaction_count,
                                SUM(amount) as total_spent,
                                MAX(sale_date) as last_transaction_date,
                                DATEDIFF(CURRENT_DATE(), MAX(sale_date)) as days_since_last_purchase
                            FROM product_sales
                            GROUP BY user_id
                        )
                        SELECT 
                            c.user_id,
                            c.customer_type,
                            COALESCE(m.transaction_count, 0) as transaction_count,
                            COALESCE(m.total_spent, 0) as total_spent,
                            CASE 
                                WHEN m.total_spent > 5000 THEN 'High Value'
                                WHEN m.total_spent > 1000 THEN 'Medium Value'
                                WHEN m.total_spent > 0 THEN 'Low Value'
                                ELSE 'No Purchases'
                            END as value_segment,
                            CASE
                                WHEN m.days_since_last_purchase <= 30 THEN 'Active'
                                WHEN m.days_since_last_purchase <= 90 THEN 'At Risk'
                                WHEN m.days_since_last_purchase <= 180 THEN 'Dormant'
                                WHEN m.days_since_last_purchase > 180 THEN 'Lost'
                                ELSE 'Never Purchased'
                            END as activity_segment
                        FROM user_profiles c
                        LEFT JOIN customer_metrics m ON c.user_id = m.user_id
                    """.strip()
                }
            },
            "gold_queries": {
                "executive_dashboard": {
                    "description": "Executive summary metrics for dashboard",
                    "query": """
                        SELECT 
                            'Total Revenue' as metric,
                            SUM(total_sales) as value,
                            'Currency' as unit
                        FROM daily_sales_silver
                        UNION ALL
                        SELECT 
                            'Total Customers' as metric,
                            COUNT(DISTINCT user_id) as value,
                            'Count' as unit
                        FROM customer_lifetime_value_silver
                        UNION ALL
                        SELECT 
                            'Avg Order Value' as metric,
                            AVG(avg_order_value) as value,
                            'Currency' as unit
                        FROM customer_lifetime_value_silver
                        WHERE total_transactions > 0
                    """.strip()
                },
                "monthly_trends": {
                    "description": "Monthly sales trends for time series analysis",
                    "query": """
                        SELECT 
                            DATE_TRUNC('month', date) as month,
                            SUM(total_sales) as monthly_revenue,
                            SUM(num_transactions) as monthly_transactions,
                            AVG(avg_transaction_value) as avg_transaction_value,
                            COUNT(DISTINCT date) as active_days
                        FROM daily_sales_silver
                        GROUP BY DATE_TRUNC('month', date)
                        ORDER BY month
                    """.strip()
                }
            }
        }
        
        # Save SQL queries to JSON file
        sql_queries_path = Path("sql_queries.json")
        with open(sql_queries_path, 'w') as f:
            json.dump(sql_queries, f, indent=2)
        
        logger.info(f"Saved SQL queries to {sql_queries_path}")
        
        # Log summary statistics
        logger.info("\n=== Pipeline Summary ===")
        logger.info(f"Total Users: {len(data_model.get_dataset('user_profiles').data):,}")
        logger.info(f"Total Transactions: {len(data_model.get_dataset('product_sales').data):,}")
        
        logger.info("\nSales demo pipeline completed successfully!")
        logger.info(f"\n📁 Raw data saved to: {base_data_dir.absolute()}")
        logger.info(f"📄 SQL queries saved to: {sql_queries_path.absolute()}")
        
        logger.info("\nGenerated datasets:")
        for dataset in data_model.datasets:
            dataset_dir = base_data_dir / dataset.name
            logger.info(f"  - {dataset.name}: {dataset_dir / f'{dataset.name}.csv'}")
        
        logger.info(f"\nSQL query categories available:")
        logger.info(f"  - Silver layer: {len(sql_queries['silver_queries'])} queries")
        logger.info(f"  - Gold layer: {len(sql_queries['gold_queries'])} queries")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        logger.exception("Full error details:")
        sys.exit(1)

if __name__ == "__main__":
    main()