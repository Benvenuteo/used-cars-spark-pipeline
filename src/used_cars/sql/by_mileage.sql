SELECT CASE WHEN mileage < 10000 THEN '0–10k'
            WHEN mileage < 50000 THEN '10–50k'
            WHEN mileage < 100000 THEN '50–100k'
            WHEN mileage < 150000 THEN '100–150k'
            ELSE '150k+' END AS mileage_band_miles,
       COUNT(*) AS listings,
       PERCENTILE_APPROX(price, 0.5, 10000) AS median_price_usd
FROM cars
GROUP BY 1
