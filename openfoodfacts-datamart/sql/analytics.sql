-- Top 10 marques par nombre de produits
SELECT b.brand_name, COUNT(*) AS nb_products
FROM dim_product p
JOIN dim_brand b ON p.brand_id = b.brand_id
GROUP BY b.brand_name
ORDER BY nb_products DESC
LIMIT 10;

-- Répartition Nutriscore par pays
SELECT c.country_code, p.nutriscore_grade, COUNT(*) AS total
FROM dim_product p
JOIN dim_country c ON p.country_id = c.country_id
GROUP BY c.country_code, p.nutriscore_grade
ORDER BY c.country_code, total DESC;

-- Produits les plus sucrés
SELECT product_name, sugars_100g
FROM dim_product
WHERE sugars_100g IS NOT NULL
ORDER BY sugars_100g DESC
LIMIT 10;
