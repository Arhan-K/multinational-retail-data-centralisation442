UPDATE dim_products
SET product_price = CAST(REPLACE(product_price, '£', '') AS NUMERIC),
SELECT product_price FROM dim_products;
--ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(10);
--UPDATE dim_products
--SET weight_classification = 
--    CASE 
--        WHEN weight_range < 2 THEN 'Light'
--       WHEN weight_range >=2 AND weight_range < 40 THEN 'Mid-Sized'
  --      WHEN weight_range >=40 and < 140 THEN 'Heavy'
    --    ELSE 'Truck Required'
    --END;
--ALTER TABLE dim_products
--ALTER COLUMN product_price TYPE FLOAT,
--ALTER COLUMN weight TYPE FLOAT,
--ALTER COLUMN EAN TYPE VARCHAR(10),
--ALTER COLUMN product_code TYPE VARCHAR(10),
--ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
--ALTER COLUMN uuid TYPE UUID,
--ALTER COLUMN still_available TYPE BOOL,
--ALTER COLUMN weight_class TYPE VARCHAR(10);