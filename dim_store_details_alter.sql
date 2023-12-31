SELECT * FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING NULLIF(latitude, 'N/A')::FLOAT,
ALTER COLUMN longitude TYPE FLOAT USING NULLIF(longitude, 'N/A')::FLOAT;

ALTER TABLE dim_store_details
ADD COLUMN coordinates TEXT;
UPDATE dim_store_details
SET coordinates = latitude || ', ' || longitude;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(255),
--ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ADD CONSTRAINT store_constraint PRIMARY KEY (store_code);

SELECT country_code, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY country_code
ORDER BY occurrence DESC;

ALTER TABLE dim_store_details ADD COLUMN city VARCHAR(255);
UPDATE dim_store_details
SET city = SPLIT_PART(SPLIT_PART(address, ', ', 2), ', ', 1);
SELECT city FROM dim_store_details;

SELECT city, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY city
ORDER BY occurrence DESC;

SELECT locality, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY locality
ORDER BY occurrence DESC;

SELECT store_type, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY store_type
ORDER BY occurrence DESC;

ALTER TABLE dim_store_details ADD COLUMN store_type VARCHAR(255);
UPDATE dim_store_details
SET store_type = SPLIT_PART(SPLIT_PART(store_code, ', ', 1), ', ', 1);
SELECT store_type FROM dim_store_details;

