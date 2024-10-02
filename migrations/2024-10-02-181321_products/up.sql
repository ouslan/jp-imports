-- Your SQL goes here
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    hts_id BIGINT REFERENCES hts(hts_id),
    naics_id BIGINT REFERENCES naics(naics_id)
);
