CREATE DATABASE IF NOT EXISTS off_dm;
USE off_dm;

-- =========================
-- DIMENSIONS
-- =========================

CREATE TABLE IF NOT EXISTS dim_time (
  time_sk INT AUTO_INCREMENT PRIMARY KEY,
  date_value DATE NOT NULL,
  year INT,
  month INT,
  day INT,
  week INT,
  UNIQUE KEY uq_dim_time_date (date_value)
);

CREATE TABLE IF NOT EXISTS dim_brand (
  brand_sk INT AUTO_INCREMENT PRIMARY KEY,
  brand_name VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_brand (brand_name)
);

CREATE TABLE IF NOT EXISTS dim_country (
  country_sk INT AUTO_INCREMENT PRIMARY KEY,
  country_code VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_dim_country (country_code)
);

CREATE TABLE IF NOT EXISTS dim_category (
  category_sk INT AUTO_INCREMENT PRIMARY KEY,
  category_code VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_category (category_code)
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_sk INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(64) NOT NULL,
  product_name VARCHAR(500),
  brand_sk INT,
  primary_category_sk INT,
  country_sk INT,
  effective_from DATE,
  effective_to DATE,
  is_current TINYINT NOT NULL DEFAULT 1,
  KEY idx_product_code (code),
  UNIQUE KEY uq_product_code_current (code, is_current),
  CONSTRAINT fk_product_brand FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk),
  CONSTRAINT fk_product_category FOREIGN KEY (primary_category_sk) REFERENCES dim_category(category_sk),
  CONSTRAINT fk_product_country FOREIGN KEY (country_sk) REFERENCES dim_country(country_sk)
);

CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
  fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  product_sk INT NOT NULL,
  time_sk INT NOT NULL,

  energy_100g DOUBLE,
  fat_100g DOUBLE,
  saturated_fat_100g DOUBLE,
  sugars_100g DOUBLE,
  salt_100g DOUBLE,
  proteins_100g DOUBLE,
  fiber_100g DOUBLE,
  sodium_100g DOUBLE,

  nutriscore_grade VARCHAR(10),
  nova_group VARCHAR(10),
  ecoscore_grade VARCHAR(10),

  completeness_score DOUBLE,
  quality_issues_json JSON,

  KEY idx_fact_product_time (product_sk, time_sk),
  CONSTRAINT fk_fact_product FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
  CONSTRAINT fk_fact_time FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk)
);
