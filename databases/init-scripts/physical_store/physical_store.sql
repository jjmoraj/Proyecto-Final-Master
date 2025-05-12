BEGIN;
DROP TRIGGER IF EXISTS trigger_set_updated_at ON physical_orders;
DROP FUNCTION IF EXISTS set_updated_at();
DROP TABLE IF EXISTS physical_orders;
CREATE TABLE physical_orders (
    order_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_purchased VARCHAR(255),
    category VARCHAR(100),
    purchase_amount_usd DECIMAL(10, 2),
    location VARCHAR(100),
    size VARCHAR(50),
    color VARCHAR(50),
    season VARCHAR(50),
    discount_applied BOOLEAN,
    promo_code_used BOOLEAN,
    payment_method VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at := NOW();
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trigger_set_updated_at BEFORE
UPDATE ON physical_orders FOR EACH ROW EXECUTE FUNCTION set_updated_at();
-- ► Aquí: ruta corregida al CSV en la raíz de /docker-entrypoint-initdb.d/
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
    payment_method,
    created_at,
    updated_at
)
FROM '/docker-entrypoint-initdb.d/physical_store.csv' WITH (FORMAT CSV, HEADER TRUE);
COMMIT;