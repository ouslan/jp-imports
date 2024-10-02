-- Your SQL goes here
CREATE TABLE hts (
    hts_id SERIAL PRIMARY KEY,
    current_year BIGINT,
    current_code VARCHAR(255),
    current_description TEXT,
    year BIGINT,
    code VARCHAR(255)
);
