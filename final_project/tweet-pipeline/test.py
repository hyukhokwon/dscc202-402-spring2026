# Databricks notebook source
# Set correct catalog and schema
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

import time

# 1. Check table exists
tables = spark.sql("SHOW TABLES LIKE 'tweets_bronze'")
assert tables.count() > 0, "❌ Table tweets_bronze does NOT exist"
print("✅ Table exists: tweets_bronze")


# 2. Wait for data (streaming-safe)
row_count = 0
for i in range(5):
    row_count = spark.sql("SELECT COUNT(*) as cnt FROM tweets_bronze").collect()[0]["cnt"]
    if row_count > 0:
        break
    print("⏳ Waiting for data...")
    time.sleep(5)

assert row_count > 0, "❌ No data found in tweets_bronze after waiting"
print(f"✅ Row count: {row_count}")


# 3. Check required columns exist
df = spark.table("tweets_bronze")
columns = df.columns

required_columns = ["date", "user", "text", "sentiment"]
missing_cols = [c for c in required_columns if c not in columns]
assert len(missing_cols) == 0, f"❌ Missing columns: {missing_cols}"
print("✅ All JSON fields present")


# 4. Check metadata columns exist
metadata_columns = ["source_file", "processing_time"]
missing_meta = [c for c in metadata_columns if c not in columns]
assert len(missing_meta) == 0, f"❌ Missing metadata columns: {missing_meta}"
print("✅ Metadata columns present")


# 5. Check metadata columns are populated (allow small delay)
null_count = spark.sql("""
SELECT COUNT(*) as cnt
FROM tweets_bronze
WHERE source_file IS NULL OR processing_time IS NULL
""").collect()[0]["cnt"]

assert null_count < 10, f"❌ Too many rows with missing metadata: {null_count}"
print("✅ Metadata columns populated (or nearly complete)")


# 6. Optional: Check near-complete ingestion
if row_count >= 40000:
    print("✅ Data ingestion looks complete (~50k rows expected)")
else:
    print("⚠️ Data still loading — row count lower than expected")


# 7. Final success message
print("🎉 ALL VALIDATIONS PASSED")

# COMMAND ----------

spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

from pyspark.sql.types import TimestampType

# 1. Compare row counts
bronze_count = spark.sql("SELECT COUNT(*) AS cnt FROM tweets_bronze").collect()[0]["cnt"]
silver_count = spark.sql("SELECT COUNT(*) AS cnt FROM tweets_silver").collect()[0]["cnt"]

assert silver_count >= bronze_count, f"❌ Silver row count ({silver_count}) is not >= Bronze ({bronze_count})"
print(f"✅ Row count check passed: Silver ({silver_count}) >= Bronze ({bronze_count})")

# 2. cleaned_text has no @mentions
mentions_in_clean = spark.sql(r"""
SELECT COUNT(*) AS cnt
FROM tweets_silver
WHERE cleaned_text RLIKE '@[\\w]+'
""").collect()[0]["cnt"]

assert mentions_in_clean == 0, f"❌ Found {mentions_in_clean} rows with @mentions still in cleaned_text"
print("✅ cleaned_text has no @mentions")

# 3. mention column is lowercase
non_lowercase = spark.sql("""
SELECT COUNT(*) AS cnt
FROM tweets_silver
WHERE mention IS NOT NULL AND mention != lower(mention)
""").collect()[0]["cnt"]

assert non_lowercase == 0, f"❌ Found {non_lowercase} non-lowercase mentions"
print("✅ All mentions are lowercase")

# 4. tweets without mentions are preserved
null_mentions = spark.sql("""
SELECT COUNT(*) AS cnt
FROM tweets_silver
WHERE mention IS NULL
""").collect()[0]["cnt"]

assert null_mentions > 0, "❌ No rows with NULL mentions — tweets without mentions may have been dropped"
print(f"✅ Tweets without mentions preserved ({null_mentions} rows)")

# 5. timestamp is TimestampType
schema = spark.table("tweets_silver").schema
timestamp_field = [f for f in schema.fields if f.name == "timestamp"][0]

assert isinstance(timestamp_field.dataType, TimestampType), (
    f"❌ timestamp column is not TimestampType (found {timestamp_field.dataType})"
)
print("✅ timestamp column is correct type (TimestampType)")

print("🎉 ALL SILVER VALIDATIONS PASSED")

# COMMAND ----------

# Set correct catalog/schema
spark.sql("USE CATALOG workspace")
spark.sql("USE SCHEMA default")

from pyspark.sql.types import DoubleType

# 1. Row count matches Silver
silver_count = spark.sql("SELECT COUNT(*) as cnt FROM tweets_silver").collect()[0]["cnt"]
gold_count = spark.sql("SELECT COUNT(*) as cnt FROM tweets_gold").collect()[0]["cnt"]

assert gold_count == silver_count, f"❌ Gold row count ({gold_count}) != Silver ({silver_count})"
print(f"✅ Row count matches Silver: {gold_count}")


# 2. predicted_score in range 0–100
invalid_scores = spark.sql("""
SELECT COUNT(*) as cnt
FROM tweets_gold
WHERE predicted_score < 0 OR predicted_score > 100 OR predicted_score IS NULL
""").collect()[0]["cnt"]

assert invalid_scores == 0, f"❌ Found {invalid_scores} invalid predicted_score values"
print("✅ predicted_score is within 0–100 range")


# 3. predicted_sentiment values valid
invalid_labels = spark.sql("""
SELECT COUNT(*) as cnt
FROM tweets_gold
WHERE predicted_sentiment NOT IN ('negative', 'neutral', 'positive')
   OR predicted_sentiment IS NULL
""").collect()[0]["cnt"]

assert invalid_labels == 0, f"❌ Found {invalid_labels} invalid predicted_sentiment values"
print("✅ predicted_sentiment values are valid")


# 4. sentiment_id and predicted_sentiment_id are binary (0 or 1)
invalid_ids = spark.sql("""
SELECT COUNT(*) as cnt
FROM tweets_gold
WHERE sentiment_id NOT IN (0,1)
   OR predicted_sentiment_id NOT IN (0,1)
""").collect()[0]["cnt"]

assert invalid_ids == 0, f"❌ Found {invalid_ids} invalid binary IDs"
print("✅ sentiment_id and predicted_sentiment_id are valid (0 or 1)")


# 5. No null predictions
null_predictions = spark.sql("""
SELECT COUNT(*) as cnt
FROM tweets_gold
WHERE predicted_sentiment IS NULL OR predicted_score IS NULL
""").collect()[0]["cnt"]

assert null_predictions == 0, f"❌ Found {null_predictions} rows with missing predictions"
print("✅ All rows have predictions")


# 6. Optional: Inspect schema
df = spark.table("tweets_gold")
print("🔍 Schema:")
df.printSchema()


# 7. Final success
print("🎉 ALL GOLD VALIDATIONS PASSED")
