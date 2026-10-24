SELECT make_name, COUNT(*) AS listings,
       AVG(price) AS mean_price_usd,
       PERCENTILE_APPROX(price, 0.5, 10000) AS median_price_usd,
       AVG(mileage) AS mean_mileage_miles
FROM cars
GROUP BY make_name
ORDER BY listings DESC, make_name
LIMIT 20
