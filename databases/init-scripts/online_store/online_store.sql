-- Create STAGING TABLE
-- Temporary table for loading raw CSV data.
CREATE TABLE staging_online_store (
    customer_id INT,
    age INT,
    gender VARCHAR(10),
    previous_purchases INT,
    item_purchased VARCHAR(255),
    category VARCHAR(100),
    purchase_amount_usd DECIMAL(10, 2),
    location VARCHAR(100),
    size VARCHAR(50),
    color VARCHAR(50),
    season VARCHAR(50),
    review_rating DECIMAL(2, 1),
    shipping_type VARCHAR(50),
    discount_applied BOOLEAN,
    promo_code_used BOOLEAN,
    payment_method VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
-- Load data from CSV into staging table
-- Make sure the path and permissions are correct.
COPY staging_online_store (
    customer_id,
    age,
    gender,
    previous_purchases,
    item_purchased,
    category,
    purchase_amount_usd,
    location,
    size,
    color,
    season,
    review_rating,
    shipping_type,
    discount_applied,
    promo_code_used,
    payment_method
)
FROM '/docker-entrypoint-initdb.d/online_store.csv' WITH (FORMAT csv, HEADER true);
-- Create TABLE customers
-- Added created_at and updated_at with triggers for completeness.
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    previous_purchases INT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Automatically set if not provided
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
-- Create TABLE online_orders
-- This table references the customers table and includes created_at, updated_at.
CREATE TABLE online_orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    item_purchased VARCHAR(255),
    category VARCHAR(100),
    purchase_amount_usd DECIMAL(10, 2),
    location VARCHAR(100),
    size VARCHAR(50),
    color VARCHAR(50),
    season VARCHAR(50),
    review_rating DECIMAL(2, 1),
    shipping_type VARCHAR(50),
    discount_applied BOOLEAN,
    promo_code_used BOOLEAN,
    payment_method VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
-- Create a function to set 'updated_at' on row updates
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Create triggers for both 'customers' and 'online_orders'
-- so that 'updated_at' is updated each time a row changes.
CREATE TRIGGER trg_set_updated_at_customers BEFORE
UPDATE ON customers FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
CREATE TRIGGER trg_set_updated_at_online_orders BEFORE
UPDATE ON online_orders FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
-- Insert DISTINCT customer data into 'customers'
-- This ensures we don't create duplicate customers if the CSV has repeated IDs.
INSERT INTO customers (
        customer_id,
        age,
        gender,
        previous_purchases,
        created_at
    )
SELECT DISTINCT customer_id,
    age,
    gender,
    previous_purchases,
    created_at -- This will overwrite the default 'NOW()'
FROM staging_online_store;
-- Insert order data into 'online_orders'
-- We're also inserting the 'created_at' from staging; the default is only used if this column isn't provided.
INSERT INTO online_orders (
        customer_id,
        item_purchased,
        category,
        purchase_amount_usd,
        location,
        size,
        color,
        season,
        review_rating,
        shipping_type,
        discount_applied,
        promo_code_used,
        payment_method,
        created_at
    )
SELECT customer_id,
    item_purchased,
    category,
    purchase_amount_usd,
    location,
    size,
    color,
    season,
    review_rating,
    shipping_type,
    discount_applied,
    promo_code_used,
    payment_method,
    created_at
FROM staging_online_store;
-- Drop staging table (no longer needed after data is loaded).
DROP TABLE staging_online_store;