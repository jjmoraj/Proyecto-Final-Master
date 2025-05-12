import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
import numpy as np

# --- Configure module-level logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def filter_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Retain only specified columns in the DataFrame.

    Args:
        df: Input DataFrame.
        columns: List of column names to keep.
    Returns:
        A new DataFrame containing only the requested columns.
    """
    missing = set(columns) - set(df.columns)
    if missing:
        logger.error("Missing columns for filter: %s", missing)
        raise KeyError(f"Columns not found in DataFrame: {missing}")
    return df.loc[:, columns].copy()


def rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Rename DataFrame columns according to given mapping.

    Args:
        df: Input DataFrame.
        mapping: Dict mapping existing names to new names.
    Returns:
        A new DataFrame with columns renamed.
    """
    return df.rename(columns=mapping, inplace=False)


def apply_physical_store_location(df: pd.DataFrame, location: str = 'Tennessee') -> pd.DataFrame:
    """
    Assign a fixed physical store location to each row.

    Args:
        df: Input DataFrame.
        location: Store location value to assign.
    Returns:
        A new DataFrame with the 'location' column updated.
    """
    return df.assign(location=location)


def generate_timestamps(
    count: int,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    seed: Optional[int] = None
) -> pd.Series:
    """
    Generate random timestamps between start_date and end_date.

    Args:
        count: Number of timestamps to generate.
        start_date: Lower bound datetime (inclusive).
        end_date: Upper bound datetime (inclusive).
        seed: Random seed for reproducibility.
    Returns:
        A pandas Series of ISO-formatted datetime strings.
    """
    rng = np.random.default_rng(seed)
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=90)

    # Convert to UNIX timestamps
    start_ts = start_date.timestamp()
    end_ts = end_date.timestamp()
    random_ts = rng.uniform(start_ts, end_ts, size=count)
    dt_index = pd.to_datetime(random_ts, unit='s')
    return dt_index.strftime("%Y-%m-%d %H:%M:%S")


def process_store_data(
    df: pd.DataFrame,
    columns: List[str],
    column_mapping: Dict[str, str],
    assign_physical: bool = False,
    physical_location: str = 'Tennessee'
) -> pd.DataFrame:
    """
    Pipeline to filter, rename, and optionally assign physical store location.

    Args:
        df: Input DataFrame.
        columns: Columns to retain before renaming.
        column_mapping: Mapping of old to new column names.
        assign_physical: If True, adds a fixed location column.
        physical_location: Value to use for the location column.
    Returns:
        Transformed DataFrame.
    """
    logger.info(
        "Processing store data (physical=%s) with %d rows", assign_physical, len(df)
    )
    result = (
        df
        .pipe(filter_columns, columns)
        .pipe(rename_columns, column_mapping)
    )
    if assign_physical:
        result = apply_physical_store_location(result, physical_location)
    return result


def main():
    """
    Main entry point: load CSV, generate timestamps, split, transform, and save outputs.
    """
    # Paths
    input_path = Path('./databases/dataset/shopping_trends.csv')
    online_output = Path('./databases/init-scripts/online_store/online_store.csv')
    physical_output = Path('./databases/init-scripts/physical_store/physical_store.csv')

    # Load and shuffle
    logger.info("Loading dataset from %s", input_path)
    df = pd.read_csv(input_path)
    df_shuffled = df.sample(frac=1, random_state=0).reset_index(drop=True)

    # Generate timestamps
    logger.info("Generating created_at and updated_at timestamps")
    yesterday = datetime.now() - timedelta(days=1)
    timestamps = generate_timestamps(
        count=len(df_shuffled),
        start_date=yesterday - timedelta(days=90),
        end_date=yesterday,
        seed=0
    )
    df_shuffled['created_at'] = timestamps
    df_shuffled['updated_at'] = timestamps

    # Split into online vs. physical
    half = len(df_shuffled) // 2
    df_online = df_shuffled.iloc[:half]
    df_physical = df_shuffled.iloc[half:]

    # Define schemas
    online_columns = [
        'Customer ID', 'Age', 'Gender', 'Previous Purchases',
        'Item Purchased', 'Category', 'Purchase Amount (USD)',
        'Location', 'Size', 'Color', 'Season', 'Review Rating',
        'Shipping Type', 'Discount Applied', 'Promo Code Used',
        'Payment Method', 'created_at', 'updated_at'
    ]
    online_column_mapping = {
        'Customer ID': 'customer_id',
        'Age': 'age',
        'Gender': 'gender',
        'Previous Purchases': 'previous_purchases',
        'Item Purchased': 'item_purchased',
        'Category': 'category',
        'Purchase Amount (USD)': 'purchase_amount_usd',
        'Location': 'location',
        'Size': 'size',
        'Color': 'color',
        'Season': 'season',
        'Review Rating': 'review_rating',
        'Shipping Type': 'shipping_type',
        'Discount Applied': 'discount_applied',
        'Promo Code Used': 'promo_code_used',
        'Payment Method': 'payment_method'
    }
    physical_columns = [
        'Item Purchased', 'Category', 'Purchase Amount (USD)',
        'Location', 'Size', 'Color', 'Season',
        'Discount Applied', 'Promo Code Used',
        'Payment Method', 'created_at', 'updated_at'
    ]
    physical_column_mapping = {
        key: val for key, val in online_column_mapping.items()
        if key in physical_columns
    }

    # Process and save
    logger.info("Transforming and saving online store data to %s", online_output)
    online_processed = process_store_data(
        df_online, online_columns, online_column_mapping, assign_physical=False
    )
    online_output.parent.mkdir(parents=True, exist_ok=True)
    online_processed.to_csv(online_output, index=False)

    logger.info("Transforming and saving physical store data to %s", physical_output)
    physical_processed = process_store_data(
        df_physical, physical_columns, physical_column_mapping,
        assign_physical=True, physical_location='Tennessee'
    )
    physical_output.parent.mkdir(parents=True, exist_ok=True)
    physical_processed.to_csv(physical_output, index=False)

    logger.info("Data processing complete. Online rows: %d, Physical rows: %d", \
                len(online_processed), len(physical_processed))


if __name__ == "__main__":
    main()
