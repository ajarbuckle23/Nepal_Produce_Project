CREATE TABLE produce_prices (
	sn INTEGER,
	item VARCHAR(255),
	price_date DATE,
	unit VARCHAR(255),
	min_price DOUBLE,
	max_price DOUBLE,
	avg_price DOUBLE
);

# Initial check to see if data import has worked 
SELECT * FROM produce_prices; 

# Checking to see if all rows have been successfully imported 
SELECT * FROM produce_prices;
SELECT COUNT(*) FROM produce_prices;

# How many different produce items are in this dataset?
SELECT COUNT(DISTINCT item) FROM produce_prices;

# What are the top 10 produce items with the most price information?
SELECT 
	item, 
	COUNT(*) AS num_recordings
FROM produce_prices
GROUP BY item
ORDER BY COUNT(*) DESC
LIMIT 10;

# What are the top 10 produce items with the least price information?
SELECT 
	item, 
	COUNT(*) AS num_recordings
FROM produce_prices
GROUP BY item
ORDER BY COUNT(*) ASC
LIMIT 10;

# What time period does this data span? 
SELECT 
	MIN(price_date) AS earliest_recording_date, 
	MAX(price_date) AS latest_recording_date
FROM produce_prices pp;

# Are the prices in this dataset all by kg? 
SELECT 
	COUNT(DISTINCT unit) AS num_distinct_units
FROM produce_prices;

# Not all of the items in this dataset are measured in kg
# How many produce items are measured in each unit measurement?
SELECT 
	unit, 
	COUNT(DISTINCT item)
FROM produce_prices pp
GROUP BY unit;

# Only four produce items are not measured in kg
# Which are they, and what units are they measured in?
SELECT
	DISTINCT item,
	unit
FROM produce_prices
WHERE unit NOT LIKE 'kg';



# DERIVED TABLES 



# GENERAL INFORMATION ABOUT EACH ITEM



SELECT 
	item, 
	unit, 
	COUNT(item) AS num_recordings
FROM produce_prices pp 
GROUP BY item, unit



# SEASONALITY INFORMATION 



# What is the month with the highest average of daily average price for each item? 
# We have to get start from this, which gives us the average price for each month, and a rank for each month

SELECT 
	item, 
	MONTH(price_date) AS month,
	AVG(avg_price) AS avg_avg_price,
	ROW_NUMBER() OVER (PARTITION BY item ORDER BY AVG(avg_price) DESC) AS price_rank
FROM produce_prices pp 
GROUP BY item, month;

# Now we can get the information we want using a CTE 

WITH monthly_averages AS (
	SELECT 
		item, 
		MONTH(price_date) AS month,
		AVG(avg_price) AS avg_avg_price,
		ROW_NUMBER() OVER (PARTITION BY item ORDER BY AVG(avg_price) DESC) AS rn
	FROM produce_prices pp 
	GROUP BY item, month
)

SELECT 
	item, 
	month AS peak_price_month,
	avg_avg_price AS price
FROM monthly_averages
WHERE rn = 1; 

# We can apply the same idea to get the least expensive months 

WITH monthly_averages AS (
	SELECT 
		item, 
		MONTH(price_date) AS month,
		AVG(avg_price) AS avg_avg_price,
		ROW_NUMBER() OVER (PARTITION BY item ORDER BY AVG(avg_price) ASC) AS rn
	FROM produce_prices pp 
	GROUP BY item, month
)

SELECT 
	item, 
	month AS low_price_month,
	avg_avg_price AS price
FROM monthly_averages
WHERE rn = 1;

# Now I can write one query that returns all the information I want at once, plus the peak and low seasons, plus 
# a metric that quantifies how much the prices for the items tend to vary between seasons 

WITH low_monthly_averages AS (
    SELECT 
        item, 
        MONTH(price_date) AS month,
        AVG(avg_price) AS avg_avg_price,
        ROW_NUMBER() OVER (PARTITION BY item ORDER BY AVG(avg_price) ASC) AS rn
    FROM produce_prices pp 
    GROUP BY item, month
),
high_monthly_averages AS (
    SELECT 
        item, 
        MONTH(price_date) AS month,
        AVG(avg_price) AS avg_avg_price,
        ROW_NUMBER() OVER (PARTITION BY item ORDER BY AVG(avg_price) DESC) AS rn
    FROM produce_prices pp 
    GROUP BY item, month
),
lpt AS (
    SELECT 
        item, 
        month AS low_price_month,
        avg_avg_price AS price
    FROM low_monthly_averages
    WHERE rn = 1
),
hpt AS (
    SELECT 
        item, 
        month AS peak_price_month,
        avg_avg_price AS price
    FROM high_monthly_averages
    WHERE rn = 1
)
SELECT 
    lpt.item AS item, 
    MONTHNAME(DATE_ADD('2000-01-01', INTERVAL lpt.low_price_month - 1 MONTH)) AS low_price_month, 
     CASE 
        WHEN lpt.low_price_month IN (12, 1, 2) THEN 'Winter'
        WHEN lpt.low_price_month IN (3, 4, 5) THEN 'Spring'
        WHEN lpt.low_price_month IN (6, 7, 8) THEN 'Summer'
        WHEN lpt.low_price_month IN (9, 10, 11) THEN 'Fall'
    END AS low_price_season,
    ROUND(lpt.price, 2) AS low_price, 
    MONTHNAME(DATE_ADD('2000-01-01', INTERVAL hpt.peak_price_month - 1 MONTH)) AS peak_price_month, 
     CASE 
        WHEN hpt.peak_price_month IN (12, 1, 2) THEN 'Winter'
        WHEN hpt.peak_price_month IN (3, 4, 5) THEN 'Spring'
        WHEN hpt.peak_price_month IN (6, 7, 8) THEN 'Summer'
        WHEN hpt.peak_price_month IN (9, 10, 11) THEN 'Fall'
    END AS peak_price_season,
    ROUND(hpt.price, 2) AS peak_price,
    ROUND((hpt.price / lpt.price), 2) AS seasonal_variability
FROM lpt 
JOIN hpt ON hpt.item = lpt.item
ORDER BY seasonal_variability DESC;






	













