USE off_dm;

DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_category;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_brand;

CREATE TABLE dim_brand (
  brand_id INT AUTO_INCREMENT PRIMARY KEY,
  brand_name VARCHAR(255),
  brand_key VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_brand (brand_key)
);

CREATE TABLE dim_country (
  country_id INT AUTO_INCREMENT PRIMARY KEY,
  country_code VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_dim_country (country_code)
);

CREATE TABLE dim_category (
  category_id INT AUTO_INCREMENT PRIMARY KEY,
  category_code VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_category (category_code)
);

CREATE TABLE dim_time (
  time_id INT AUTO_INCREMENT PRIMARY KEY,
  date_value DATE NOT NULL,
  year INT, month INT, day INT, week INT,
  UNIQUE KEY uq_dim_time (date_value)
);

CREATE TABLE dim_product (
  product_id INT AUTO_INCREMENT PRIMARY KEY,
  product_code VARCHAR(64) NOT NULL,
  product_name TEXT,
  brand_id INT,
  country_id INT,
  category_id INT,
  time_id INT,
  nutriscore_grade VARCHAR(16),
  ecoscore_grade VARCHAR(16),
  nova_group VARCHAR(16),
  energy_100g DOUBLE,
  fat_100g DOUBLE,
  `saturated-fat_100g` DOUBLE,
  sugars_100g DOUBLE,
  salt_100g DOUBLE,
  proteins_100g DOUBLE,
  fiber_100g DOUBLE,
  sodium_100g DOUBLE,
  completeness_score DOUBLE,
  quality_issues_json TEXT,
  UNIQUE KEY uq_dim_product_code (product_code),
  CONSTRAINT fk_product_brand FOREIGN KEY (brand_id) REFERENCES dim_brand(brand_id),
  CONSTRAINT fk_product_country FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
  CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
  CONSTRAINT fk_product_time FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);
