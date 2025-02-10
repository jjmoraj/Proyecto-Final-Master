-- Create the 'physical_orders' table
-- This table will store details of physical orders made in a store.
-- 'order_id' is the primary key, automatically incremented by SERIAL.
-- It includes 'created_at' with a default of NOW(), and 'updated_at' 
-- that will be updated by a trigger on each UPDATE operation.
CREATE TABLE physical_orders (
    order_id SERIAL PRIMARY KEY,
    -- Primary Key (auto-increment)
    item_purchased VARCHAR(255),
    -- Name of the purchased item
    category VARCHAR(100),
    -- Category/type of the item
    purchase_amount_usd DECIMAL(10, 2),
    -- Amount in USD, up to 2 decimal places
    location VARCHAR(100),
    -- Location where the purchase occurred
    size VARCHAR(50),
    -- Size of the item (e.g., S, M, L)
    color VARCHAR(50),
    -- Color of the item
    season VARCHAR(50),
    -- Season (e.g., winter, summer, etc.)
    discount_applied BOOLEAN,
    -- Indicates if a discount was applied
    promo_code_used BOOLEAN,
    -- Indicates if a promo code was used
    payment_method VARCHAR(50),
    -- Payment method (e.g., credit card, cash)
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    -- Timestamp when the record is created (auto-set)
    updated_at TIMESTAMP -- Timestamp when the record is updated (set via trigger)
);
-- Create a function that sets 'updated_at' to the current timestamp
-- each time a row is updated.
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Create a trigger on the 'physical_orders' table to call the function
-- 'set_updated_at' before each UPDATE operation.
CREATE TRIGGER trigger_set_updated_at BEFORE
UPDATE ON physical_orders FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
-- Copy data from the CSV file 'physical_store.csv' into the 'physical_orders' table.
-- The CSV must have columns matching the listed columns in the same order.
-- Note that 'updated_at' will remain NULL until an UPDATE occurs.
-- Make sure the file path '/docker-entrypoint-initdb.d/physical_store.csv'
-- is correct and that the container can access the file.
COPY physical_orders (
    item_purchased,
    category,
    purchase_amount_usd,
    location,
    size,
    color,
    season,
    discount_applied,
    promo_code_used,
    payment_method
)
FROM '/docker-entrypoint-initdb.d/physical_store.csv' WITH (FORMAT csv, HEADER true);