BEGIN;
DROP TRIGGER IF EXISTS trg_set_updated_at_customers ON customers;
DROP TRIGGER IF EXISTS trg_set_updated_at_online_orders ON online_orders;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TABLE IF EXISTS staging_online_store;
DROP TABLE IF EXISTS online_orders;
DROP TABLE IF EXISTS customers;
-- 1) Staging
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
-- 2) COPY corregido
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
    payment_method,
    created_at,
    updated_at
)
FROM '/docker-entrypoint-initdb.d/online_store.csv' WITH (FORMAT CSV, HEADER TRUE);
-- 3) Customers
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    previous_purchases INT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
-- 4) Pedidos online
CREATE TABLE online_orders (
    order_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
-- 5) Función y triggers
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at := NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_set_updated_at_customers BEFORE
UPDATE ON customers FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_set_updated_at_online_orders BEFORE
UPDATE ON online_orders FOR EACH ROW EXECUTE FUNCTION set_updated_at();
-- 6) Poblado desde staging
INSERT INTO customers (
        customer_id,
        age,
        gender,
        previous_purchases,
        created_at,
        updated_at
    )
SELECT DISTINCT customer_id,
    age,
    gender,
    previous_purchases,
    created_at,
    updated_at
FROM staging_online_store;
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
        created_at,
        updated_at
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
    created_at,
    updated_at
FROM staging_online_store;
-- 7) Limpia staging
DROP TABLE staging_online_store;
COMMIT;