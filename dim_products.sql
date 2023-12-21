UPDATE dim_products
SET product_price = CAST(REPLACE(product_price, '£', '') AS NUMERIC)
SELECT * FROM product_price