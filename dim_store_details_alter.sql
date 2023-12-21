ALTER TABLE dim_store_details
ADD COLUMN latitude TEXT;
UPDATE dim_store_details
SET latitude = latitude1 || ' ' || latitude2;
ALTER TABLE dim_store_details
DROP COLUMN latitude1,
DROP COLUMN latitude2;
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type TYPE VARCHAR(255) NULLABLE,
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255),