SELECT year, COUNT(*) AS listings, AVG(price) AS mean_price_usd,
       PERCENTILE_APPROX(price, 0.5, 10000) AS median_price_usd
FROM cars
GROUP BY year
ORDER BY year
