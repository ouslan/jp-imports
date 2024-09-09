-- Create Naics table
CREATE TABLE IF NOT EXISTS "naics" (
    id SERIAL PRIMARY KEY,
    code VARCHAR(6) NOT NULL,
    title VARCHAR(255) NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL
);
