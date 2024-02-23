--ALTER dim_card_details
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;
ALTER TABLE dim_card_details
ADD CONSTRAINT card_constraint PRIMARY KEY (card_number);


--ALTER dim_date_times
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
ALTER TABLE dim_date_times
ADD CONSTRAINT date_constraint PRIMARY KEY (date_uuid);


--ALTER dim_store_details
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
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ADD CONSTRAINT store_constraint PRIMARY KEY (store_code);



--ALTER dim_products
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(20),
ALTER COLUMN product_code TYPE VARCHAR(20),
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN weight_classification TYPE VARCHAR(20),
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN;
SELECT still_available FROM dim_products;

UPDATE dim_products
SET product_price = CAST(REPLACE(product_price, '£', '') AS NUMERIC);

UPDATE dim_products
SET weight = REPLACE(weight, 'kg', '');
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

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
ADD CONSTRAINT code_constraint PRIMARY KEY (product_code);



--ALTER orders_table
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN store_code TYPE VARCHAR(19),
ALTER COLUMN product_code TYPE VARCHAR(19),
ALTER COLUMN product_quantity TYPE SMALLINT;


ALTER TABLE orders_table
ADD CONSTRAINT foreign_key_date
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
FOREIGN KEY (user_uuid) REFERENCES dim_users_table(user_uuid),
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number),
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);


WITH MonthlySales AS (
    SELECT 
        dim_date_times.date_uuid,
        SUM(o.product_quantity * p.product_price) AS total_monthly_sales
    FROM 
        dim_products AS p
    JOIN
        orders_table ON orders_table.product_code = p.product_code
    JOIN 
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    GROUP BY 
        dim_date_times.month
)
SELECT 
    month,
    total_monthly_sales
FROM 
    MonthlySales
ORDER BY 
    total_monthly_sales DESC, month;



SELECT product_price FROM dim_products
SELECT product_quantity FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
SELECT SUM(orders_table.product_quantity * dim_products.product_price)
--SUM(product_quantity * product_price)
SELECT month, COUNT(*) AS occurrence
FROM dim_date_times
GROUP BY month
ORDER BY occurrence DESC;

-- Task 1: total # stores
SELECT country_code, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY country_code
ORDER BY occurrence DESC;

-- Task 2: # stores by location
SELECT locality, COUNT(*) AS occurrence
FROM dim_store_details
GROUP BY locality
ORDER BY occurrence DESC;

-- Task 3: total sales by month


-- Task 1: total sales by month
SELECT 
        dim_date_times.date_uuid,
        orders_table.product_quantity,  
        product_price
    FROM 
        dim_products
    JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid,
        dim_products ON orders_table.product_code = dim_products.product code;



-- Task 2: Total sales by store type
SELECT 
        dim_store_details.store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS TotalSales
    FROM
        orders_table
    JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY dim_store_details.store_type
    ORDER BY TotalSales DESC;

--Task 3: Monthly sales
SELECT 
        dim_date_times.month,
        SUM(orders_table.product_quantity * dim_products.product_price) AS MonthlySales
    FROM
        orders_table
    JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY dim_date_times.month
    ORDER BY MonthlySales DESC;

--Task 4: Online v Offline
SELECT
    CASE 
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online'
        ELSE 'Offline'
    END AS product_type,
    SUM(orders_table.product_quantity)
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY
    CASE 
        WHEN dim_store_details.store_type = 'Web Portal' THEN 'Online'
        ELSE 'Offline'
    END
ORDER BY sum DESC;

--Task 5: % sales
SELECT 
        dim_store_details.store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS StoreSales,
        CAST(COUNT (*) AS NUMERIC) * 100 / (SELECT CAST(COUNT (*) AS NUMERIC) FROM orders_table) AS percentage_sales        

FROM orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY StoreSales DESC;



--Task 6: Monthly sales across all years
SELECT 
        dim_date_times.month,
        dim_date_times.year,
        SUM(orders_table.product_quantity * dim_products.product_price) AS MonthlySales
    FROM
        orders_table
    JOIN
        dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    GROUP BY dim_date_times.month, dim_date_times.year
    ORDER BY MonthlySales DESC;

--Task 7: Employee headcount
SELECT country_code, SUM(staff_numbers) AS total_staff_numbers
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

--Task 8: Highest selling DE store
SELECT 
        dim_store_details.store_type,
        SUM(orders_table.product_quantity * dim_products.product_price) AS StoreSales,
        dim_store_details.country_code

FROM orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE
    country_code = 'DE'
GROUP BY store_type, country_code
ORDER BY StoreSales DESC;

--Task 9: sales rate
SELECT 
        timestamp, year
FROM dim_date_times
GROUP BY timestamp, year
ORDER BY timestamp DESC;

ALTER TABLE dim_date_times
ADD COLUMN full_timestamp TEXT;
UPDATE dim_date_times
SET full_timestamp = year || '-' || month || '-' || day || ' ' || timestamp;


WITH next_time AS (
    SELECT
        full_timestamp, year,
        LEAD(full_timestamp) OVER (ORDER BY full_timestamp) AS next_full_timestamp
    FROM
        dim_date_times),
D_TIME AS (
    SELECT
        full_timestamp, next_full_timestamp, year,
        CAST(next_full_timestamp AS TIMESTAMP) - CAST(full_timestamp AS TIMESTAMP) AS time_difference
    FROM
        next_time)
SELECT 
    year,
    AVG(time_difference) as avg_time
FROM 
    D_TIME
GROUP BY
    year
ORDER BY
    avg_time DESC, year;