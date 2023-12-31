ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
ALTER TABLE dim_date_times
ADD CONSTRAINT date_constraint PRIMARY KEY (date_uuid);

SELECT * FROM dim_date_times;
SELECT month, COUNT(*) AS occurrence
FROM dim_date_times
GROUP BY month
ORDER BY occurrence DESC;

SELECT * FROM dim_date_times;