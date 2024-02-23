ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;
ALTER TABLE dim_card_details
ADD CONSTRAINT card_constraint PRIMARY KEY (card_number);
SELECT * FROM dim_card_details;
