SELECT COUNT(*) AS listings,
       APPROX_COUNT_DISTINCT(vehicle_key) AS approximate_vehicles,
       AVG(price) AS mean_price_usd,
       PERCENTILE_APPROX(price, 0.5, 10000) AS median_price_usd,
       AVG(mileage) AS mean_mileage_miles,
       MIN(year) AS min_model_year,
       MAX(year) AS max_model_year
FROM cars
