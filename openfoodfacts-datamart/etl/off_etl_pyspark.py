# etl/off_etl_pyspark.py
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# -----------------------------
# CONFIG
# -----------------------------
MYSQL_JAR = r"C:\Users\Admin\Desktop\openfoodfacts-datamart\jars\mysql-connector-j-9.5.0.jar"

INPUT_PATH = "data/en.openfoodfacts.org.products.csv"  # OFF "CSV" en réalité TSV
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB = "off_dm"
MYSQL_USER = "root"
MYSQL_PASSWORD = "taytou25"  # ⚠️ idéalement: variable d'env

# ✅ JDBC en UTF-8 (mieux: utf8mb4 côté MySQL)
JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
    "&useUnicode=true&characterEncoding=UTF-8"
)

JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}

JDBC_WRITE_OPTIONS = {
    "batchsize": "2000",
    "isolationLevel": "READ_COMMITTED",
}

BRAND_MAX_LEN = 255
CATEGORY_MAX_LEN = 255
COUNTRY_MAX_LEN = 64

# -----------------------------
# Spark session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("OpenFoodFacts_ETL")
    .master("local[4]")  # ✅ limite threads
    .config("spark.jars", MYSQL_JAR)
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.default.parallelism", "64")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Helpers
# -----------------------------
def clean_str(c: str):
    return F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c)))

def make_safe_key(col_expr):
    """
    Clé ASCII safe pour éviter caractères transformés en '?' par JDBC/MySQL.
    """
    x = F.trim(F.lower(col_expr))
    x = F.regexp_replace(x, r"\s+", " ")

    # accents FR/latin
    x = F.regexp_replace(x, "[àáâäãå]", "a")
    x = F.regexp_replace(x, "[ç]", "c")
    x = F.regexp_replace(x, "[èéêë]", "e")
    x = F.regexp_replace(x, "[ìíîï]", "i")
    x = F.regexp_replace(x, "[ñ]", "n")
    x = F.regexp_replace(x, "[òóôöõ]", "o")
    x = F.regexp_replace(x, "[ùúûü]", "u")
    x = F.regexp_replace(x, "[ýÿ]", "y")

    # quelques lettres fréquentes
    x = F.regexp_replace(x, "[ş]", "s")
    x = F.regexp_replace(x, "[ğ]", "g")
    x = F.regexp_replace(x, "[ı]", "i")
    x = F.regexp_replace(x, "[ß]", "ss")
    x = F.regexp_replace(x, "[œ]", "oe")
    x = F.regexp_replace(x, "[æ]", "ae")
    x = F.regexp_replace(x, "[ø]", "o")
    x = F.regexp_replace(x, "[ł]", "l")

    # ASCII safe (on garde : a-z 0-9 : _ - espace)
    x = F.regexp_replace(x, r"[^a-z0-9:_\- ]", "")
    x = F.regexp_replace(x, r"\s+", " ")
    x = F.trim(x)
    return x

def make_brand_key(col_expr):
    return make_safe_key(col_expr).substr(1, BRAND_MAX_LEN)

def make_category_key(col_expr):
    x = make_safe_key(col_expr)
    x = F.regexp_replace(x, r"\s+", "")  # tags -> pas d'espace
    return x.substr(1, CATEGORY_MAX_LEN)

def make_country_key(col_expr):
    # ex: en:france
    x = F.lower(F.trim(col_expr))
    x = F.regexp_replace(x, r"\s+", "")
    x = make_safe_key(x)
    x = F.regexp_replace(x, r"\s+", "")
    return x.substr(1, COUNTRY_MAX_LEN)

# -----------------------------
# BRONZE: read TSV
# -----------------------------
df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", "\t")
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_PATH)
)

wanted = [
    "code",
    "product_name",
    "product_name_fr",
    "brands",
    "categories_tags",
    "countries_tags",
    "nutriscore_grade",
    "nova_group",
    "ecoscore_grade",
    "last_modified_t",
    "energy_100g",
    "fat_100g",
    "saturated-fat_100g",
    "sugars_100g",
    "salt_100g",
    "proteins_100g",
    "fiber_100g",
    "sodium_100g",
]
fallback_map = {"countries_tags": "countries"}

select_exprs = []
for c in wanted:
    if c in df_raw.columns:
        select_exprs.append(F.col(c).alias(c))
    elif c in fallback_map and fallback_map[c] in df_raw.columns:
        select_exprs.append(F.col(fallback_map[c]).alias(c))
    else:
        select_exprs.append(F.lit(None).cast("string").alias(c))

df_bronze = df_raw.select(*select_exprs)

df_bronze = (
    df_bronze
    .withColumn("last_modified_t", F.col("last_modified_t").cast("long"))
    .withColumn("energy_100g", F.col("energy_100g").cast("double"))
    .withColumn("fat_100g", F.col("fat_100g").cast("double"))
    .withColumn("saturated-fat_100g", F.col("saturated-fat_100g").cast("double"))
    .withColumn("sugars_100g", F.col("sugars_100g").cast("double"))
    .withColumn("salt_100g", F.col("salt_100g").cast("double"))
    .withColumn("proteins_100g", F.col("proteins_100g").cast("double"))
    .withColumn("fiber_100g", F.col("fiber_100g").cast("double"))
    .withColumn("sodium_100g", F.col("sodium_100g").cast("double"))
)

bronze_count = df_bronze.count()
print("Bronze count:", bronze_count)

# -----------------------------
# SILVER: clean + dedup
# -----------------------------
df = (
    df_bronze
    .withColumn("code", clean_str("code"))
    .withColumn("product_name", clean_str("product_name"))
    .withColumn("product_name_fr", clean_str("product_name_fr"))
    .withColumn("brands", clean_str("brands"))
    .withColumn("categories_tags", clean_str("categories_tags"))
    .withColumn("countries_tags", clean_str("countries_tags"))
    .withColumn("nutriscore_grade", clean_str("nutriscore_grade"))
    .withColumn("ecoscore_grade", clean_str("ecoscore_grade"))
)

df = df.withColumn(
    "product_name_clean",
    F.when(F.col("product_name_fr").isNotNull(), F.col("product_name_fr"))
     .when(F.col("product_name").isNotNull(), F.col("product_name"))
     .otherwise(F.lit(None))
)

df = df.filter(F.col("code").isNotNull())
df = df.filter(
    F.col("product_name_clean").isNotNull()
    | F.col("brands").isNotNull()
    | F.col("categories_tags").isNotNull()
    | F.col("countries_tags").isNotNull()
    | F.col("sugars_100g").isNotNull()
    | F.col("salt_100g").isNotNull()
    | F.col("energy_100g").isNotNull()
)

w = Window.partitionBy("code").orderBy(F.col("last_modified_t").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

silver_count = df.count()
print("Silver count (after filter + dedup):", silver_count)

# ✅ cache “safe” (moins de RAM que MEMORY_ONLY)
df = df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()

# -----------------------------
# Quality rules + metrics
# -----------------------------
anomaly_sugars = (F.col("sugars_100g") < 0) | (F.col("sugars_100g") > 100)
anomaly_salt = (F.col("salt_100g") < 0) | (F.col("salt_100g") > 25)

ratio = F.when(
    F.col("sodium_100g").isNotNull() & (F.col("sodium_100g") > 0) & F.col("salt_100g").isNotNull(),
    F.col("salt_100g") / (F.col("sodium_100g") * F.lit(2.5))
).otherwise(F.lit(None).cast("double"))

anomaly_salt_sodium = ratio.isNotNull() & ((ratio < 0.8) | (ratio > 1.2))

df = df.withColumn("has_name", F.when(F.col("product_name_clean").isNotNull(), 1).otherwise(0))
df = df.withColumn("has_brand", F.when(F.col("brands").isNotNull(), 1).otherwise(0))
df = df.withColumn("has_category", F.when(F.col("categories_tags").isNotNull(), 1).otherwise(0))
df = df.withColumn("has_country", F.when(F.col("countries_tags").isNotNull(), 1).otherwise(0))
df = df.withColumn(
    "has_nutrients",
    F.when(
        F.col("energy_100g").isNotNull()
        | F.col("sugars_100g").isNotNull()
        | F.col("salt_100g").isNotNull(),
        1
    ).otherwise(0)
)

df = df.withColumn(
    "completeness_score",
    (F.col("has_name") + F.col("has_brand") + F.col("has_category") + F.col("has_country") + F.col("has_nutrients")) / F.lit(5.0)
)

df = df.withColumn(
    "quality_issues_json",
    F.when(anomaly_sugars | anomaly_salt | anomaly_salt_sodium, F.lit('{"anomaly":true}'))
     .otherwise(F.lit('{"anomaly":false}'))
)

dup_check = df.groupBy("code").count().filter(F.col("count") > 1).count()
an_sug = df.filter(anomaly_sugars).count()
an_salt = df.filter(anomaly_salt).count()
an_ss = df.filter(anomaly_salt_sodium).count()
avg_completeness = df.select(F.avg("completeness_score").alias("avg")).collect()[0]["avg"]

metrics = {
    "input_path": INPUT_PATH,
    "bronze_count": int(bronze_count),
    "silver_count": int(silver_count),
    "duplicates_after_dedup": int(dup_check),
    "avg_completeness_score": float(avg_completeness) if avg_completeness is not None else None,
    "anomalies": {
        "sugars_out_of_bounds": int(an_sug),
        "salt_out_of_bounds": int(an_salt),
        "salt_sodium_inconsistent": int(an_ss),
    }
}

with open("quality_metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics, f, indent=2, ensure_ascii=False)

print("✅ Metrics written to quality_metrics.json")

df.select(
    "code", "product_name_clean", "brands", "countries_tags",
    "nutriscore_grade", "sugars_100g", "salt_100g",
    "completeness_score", "quality_issues_json"
).show(10, truncate=False)

# -----------------------------
# STEP 5: Load Dimensions to MySQL
# -----------------------------
# ⚠️ IMPORTANT: TRUNCATE tables avant relance pour éviter UNIQUE collisions

# dim_brand
dim_brand = (
    df.select(F.trim(F.col("brands")).alias("brand_name"))
      .where(F.col("brand_name").isNotNull() & (F.col("brand_name") != ""))
      .withColumn("brand_name", F.col("brand_name").substr(1, BRAND_MAX_LEN))
      .withColumn("brand_key", make_brand_key(F.col("brand_name")))
      .where(F.length(F.col("brand_key")) > 1)
      .dropDuplicates(["brand_key"])
      .coalesce(1)
)

(dim_brand.write.mode("append").format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "dim_brand")
 .options(**JDBC_PROPS)
 .options(**JDBC_WRITE_OPTIONS)
 .save()
)

# dim_country
dim_country = (
    df.select(F.split(F.col("countries_tags"), ",").getItem(0).alias("country_code_raw"))
      .where(F.col("country_code_raw").isNotNull() & (F.col("country_code_raw") != ""))
      .withColumn("country_code", make_country_key(F.col("country_code_raw")))
      .drop("country_code_raw")
      .where(F.col("country_code").rlike(r"^[a-z]{2}:[a-z0-9-]+$"))
      .dropDuplicates(["country_code"])
      .coalesce(1)
)

(dim_country.write.mode("append").format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "dim_country")
 .options(**JDBC_PROPS)
 .options(**JDBC_WRITE_OPTIONS)
 .save()
)

# dim_category
dim_category = (
    df.select(F.split(F.col("categories_tags"), ",").getItem(0).alias("category_code_raw"))
      .where(F.col("category_code_raw").isNotNull() & (F.col("category_code_raw") != ""))
      .withColumn("category_code", make_category_key(F.col("category_code_raw")))
      .drop("category_code_raw")
      .where(F.length(F.col("category_code")) > 3)
      .dropDuplicates(["category_code"])
      .coalesce(1)
)

(dim_category.write.mode("append").format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "dim_category")
 .options(**JDBC_PROPS)
 .options(**JDBC_WRITE_OPTIONS)
 .save()
)

# dim_time
df_time = df.withColumn("date_value", F.to_date(F.from_unixtime(F.col("last_modified_t"))))
dim_time = (
    df_time.select("date_value")
      .where(F.col("date_value").isNotNull())
      .dropDuplicates(["date_value"])
      .withColumn("year", F.year(F.col("date_value")))
      .withColumn("month", F.month(F.col("date_value")))
      .withColumn("day", F.dayofmonth(F.col("date_value")))
      .withColumn("week", F.weekofyear(F.col("date_value")))
      .coalesce(1)
)

(dim_time.write.mode("append").format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "dim_time")
 .options(**JDBC_PROPS)
 .options(**JDBC_WRITE_OPTIONS)
 .save()
)

print("✅ Dimensions chargées dans MySQL")

# -----------------------------
# Read dimensions back (for ids)
# -----------------------------
dim_brand_db = (spark.read.format("jdbc")
    .option("url", JDBC_URL).option("dbtable", "dim_brand")
    .options(**JDBC_PROPS).load()
).select("brand_id", "brand_key")

dim_country_db = (spark.read.format("jdbc")
    .option("url", JDBC_URL).option("dbtable", "dim_country")
    .options(**JDBC_PROPS).load()
).select("country_id", "country_code")

dim_category_db = (spark.read.format("jdbc")
    .option("url", JDBC_URL).option("dbtable", "dim_category")
    .options(**JDBC_PROPS).load()
).select("category_id", "category_code")

dim_time_db = (spark.read.format("jdbc")
    .option("url", JDBC_URL).option("dbtable", "dim_time")
    .options(**JDBC_PROPS).load()
).select("time_id", "date_value")

# -----------------------------
# Build product rows + join ids
# -----------------------------
df_prod = (
    df.select(
        "code", "product_name_clean", "brands", "countries_tags", "categories_tags",
        "nutriscore_grade", "ecoscore_grade", "nova_group", "last_modified_t",
        "energy_100g", "fat_100g", "saturated-fat_100g", "sugars_100g", "salt_100g",
        "proteins_100g", "fiber_100g", "sodium_100g", "completeness_score", "quality_issues_json"
    )
    .withColumn("brand_key", make_brand_key(F.col("brands")))
    .withColumn("country_key", make_country_key(F.split(F.col("countries_tags"), ",").getItem(0)))
    .withColumn("category_key", make_category_key(F.split(F.col("categories_tags"), ",").getItem(0)))
    .withColumn("date_value", F.to_date(F.from_unixtime(F.col("last_modified_t"))))
    .drop("brands", "countries_tags", "categories_tags")  # ✅ réduit taille
)

# ⚠️ Broadcast: OK pour country/time (petits). PAS forcément pour brand/category (peuvent être gros).
from pyspark.sql.functions import broadcast

prod_joined = (
    df_prod
    .join(dim_brand_db, df_prod["brand_key"] == dim_brand_db["brand_key"], "left")
    .join(broadcast(dim_country_db), df_prod["country_key"] == dim_country_db["country_code"], "left")
    .join(dim_category_db, df_prod["category_key"] == dim_category_db["category_code"], "left")
    .join(broadcast(dim_time_db), df_prod["date_value"] == dim_time_db["date_value"], "left")
)

dim_product = prod_joined.select(
    F.col("code").alias("product_code"),
    F.col("product_name_clean").alias("product_name"),
    F.col("brand_id"),
    F.col("country_id"),
    F.col("category_id"),
    F.col("time_id"),
    F.col("nutriscore_grade"),
    F.col("ecoscore_grade"),
    F.col("nova_group").cast("string"),
    F.col("energy_100g"),
    F.col("fat_100g"),
    F.col("saturated-fat_100g"),
    F.col("sugars_100g"),
    F.col("salt_100g"),
    F.col("proteins_100g"),
    F.col("fiber_100g"),
    F.col("sodium_100g"),
    F.col("completeness_score"),
    F.col("quality_issues_json"),
).repartition(32)

(dim_product.write.mode("append").format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "dim_product")
 .options(**JDBC_PROPS)
 .options(**JDBC_WRITE_OPTIONS)
 .save()
)

print("✅ dim_product chargée (append)")
spark.stop()
