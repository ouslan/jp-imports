-- Your SQL goes here
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    date DATE,
    value BIGINT,
    product_id BIGINT REFERENCES products(product_id)
);