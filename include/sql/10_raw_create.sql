-- RAW layer: an exact copy of every source file, every run.
-- All business columns are VARCHAR (blanks stay '', 'N/A' and 'UPI ' stay as written).
-- Lineage: _source_row (1-based data line in the file), _loaded_at, _source_file, _run_id.
-- Append-only: each run adds one batch per file; rows are never cleaned, updated or deleted.
-- A retry or Clear of the same run keeps the batch that is already there instead of loading it again.
-- Increment files land in raw_orders, told apart by _source_file.

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR,
    payment_mode VARCHAR, order_status VARCHAR, channel VARCHAR, delivery_minutes VARCHAR,
    _source_row BIGINT, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR,
    amount_paid VARCHAR, payment_status VARCHAR, gateway_ref VARCHAR,
    _source_row BIGINT, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR, is_active VARCHAR,
    _source_row BIGINT, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR, launch_date VARCHAR,
    _source_row BIGINT, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date VARCHAR,
    manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _source_row BIGINT, _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR
);
