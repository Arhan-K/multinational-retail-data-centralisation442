ALTER TABLE dim_store_details
UPDATE dim_store_details
ALTER TABLE dim_store_details
SELECT latitude FROM dim_store_details;
ADD COLUMN latitude_2 TEXT;
UPDATE dim_store_details
SET latitude_2 = latitude;
SELECT latitude_2 FROM dim_store_details;
ALTER TABLE dim_store_details
DROP COLUMN lat,
DROP COLUMN latitude;

ALTER TABLE dim_store_details
RENAME COLUMN latitude_2 TO latitude;
SELECT latitude FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(255),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
--ALTER COLUMN store_type TYPE VARCHAR(255) NULL,
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);