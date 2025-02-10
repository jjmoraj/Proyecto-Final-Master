import pandas as pd
from datetime import datetime

def filter_columns(df, columns):
    """
    Selects only the specified columns from the DataFrame.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.
    - columns (list): List of column names to keep.

    Returns:
    - pd.DataFrame: Filtered DataFrame.
    """
    return df[columns].copy()

def rename_columns(df, column_mapping):
    """
    Renames the columns of the DataFrame according to the provided mapping.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.
    - column_mapping (dict): Dictionary mapping old column names to new ones.

    Returns:
    - pd.DataFrame: DataFrame with renamed columns.
    """
    return df.rename(columns=column_mapping, inplace=False)

def apply_physical_store_location(df):
    """
    Assigns a fixed store location ('Tennessee') to the 'location' column.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.

    Returns:
    - pd.DataFrame: DataFrame with updated location.
    """
    return df.assign(location='Tennessee')

def process_store_data(df, columns, column_mapping, is_physical=False):
    """
    Processes store data by filtering, renaming, and optionally applying location.

    Parameters:
    - df (pd.DataFrame): Input DataFrame.
    - columns (list): List of columns to retain.
    - column_mapping (dict): Dictionary for renaming columns.
    - is_physical (bool): If True, applies physical store location.

    Returns:
    - pd.DataFrame: Transformed DataFrame.
    """
    return (
            df
            .pipe(filter_columns, columns)
            .pipe(rename_columns, column_mapping)
            .pipe(apply_physical_store_location)
            if is_physical
            else
            df
            .pipe(filter_columns, columns)
            .pipe(rename_columns, column_mapping)
        )

# Define the column mappings for online and physical stores
online_columns = [
    'Customer ID', 'Age', 'Gender', 'Previous Purchases',
    'Item Purchased', 'Category', 'Purchase Amount (USD)',
    'Location', 'Size', 'Color', 'Season', 'Review Rating',
    'Shipping Type', 'Discount Applied', 'Promo Code Used',
    'Payment Method'
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
    'Location', 'Size', 'Color', 'Season', 'Discount Applied', 
    'Promo Code Used', 'Payment Method'
]

physical_column_mapping = {
    'Item Purchased': 'item_purchased',
    'Category': 'category',
    'Purchase Amount (USD)': 'purchase_amount_usd',
    'Location': 'location',
    'Size': 'size',
    'Color': 'color',
    'Season': 'season',
    'Discount Applied': 'discount_applied',
    'Promo Code Used': 'promo_code_used',
    'Payment Method': 'payment_method',
}

if __name__ == "__main__":
    # Load dataset
    df = pd.read_csv('./databases/dataset/shopping_trends.csv')

    # Shuffle the DataFrame randomly
    df_shuffled = df.sample(frac=1, random_state=0).reset_index(drop=True)

    print(df.head())

    # Split the DataFrame into two halves
    half = len(df) // 2
    df_online, df_physical = df_shuffled.iloc[:half], df_shuffled.iloc[half:]

    # Process online and physical stores
    online_store = process_store_data(df_online, online_columns, online_column_mapping)
    physical_store = process_store_data(df_physical, physical_columns, physical_column_mapping, is_physical=True)

    # Save the transformed dataframes
    online_store.to_csv('./databases/init-scripts/online_store/online_store.csv', index=False)
    print(online_store.head())
    physical_store.to_csv('./databases/init-scripts/physical_store/physical_store.csv', index=False)
    print(physical_store.head())
