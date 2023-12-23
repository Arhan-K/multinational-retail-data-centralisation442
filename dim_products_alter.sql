UPDATE dim_products
SET product_price = CAST(REPLACE(product_price, '£', '') AS NUMERIC);
SELECT product_price FROM dim_products;
UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;
SELECT weight from dim_products;
ALTER TABLE dim_products
ADD COLUMN weight_classification VARCHAR(20);
UPDATE dim_products
SET weight_classification = 
    CASE 
       WHEN weight < 2 THEN 'Light'
       WHEN weight >= 2 AND weight < 40 THEN 'Mid-Sized'
       WHEN weight >=40 AND weight < 140 THEN 'Heavy'
       ELSE 'Truck Required'
    END;
SELECT weight FROM dim_products;
SELECT weight_classification FROM dim_products;
SELECT * FROM dim_products;
SELECT removed FROM dim_products;


ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;
UPDATE dim_products
SET still_available = 
  CASE 
    WHEN still_available = 'Still_avaliable' THEN 1
    WHEN still_available = 'Removed' THEN 0
    ELSE NULL
  END;

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(20),
ALTER COLUMN product_code TYPE VARCHAR(20),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN weight_classification TYPE VARCHAR(20),
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN;
SELECT still_available FROM dim_products;
