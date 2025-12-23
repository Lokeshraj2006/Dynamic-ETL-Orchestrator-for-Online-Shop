CREATE TABLE IF NOT EXISTS sales (
    order_id      INTEGER PRIMARY KEY,
    order_date    DATE,
    product       TEXT,
    quantity      INTEGER,
    price         NUMERIC,
    total_amount  NUMERIC,  -- quantity * price
    loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table to track failed files
CREATE TABLE IF NOT EXISTS failed_loads (
    file_name     TEXT,
    error_message TEXT,
    failed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
