-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Application Layer: Tweet Sentiment Aggregations
-- MAGIC
-- MAGIC ## Purpose
-- MAGIC Aggregate sentiment predictions by mentioned user for dashboard analytics.
-- MAGIC Pre-compute metrics to enable fast dashboard queries.
-- MAGIC
-- MAGIC ## Requirements
-- MAGIC - Create materialized view: gold_tweet_aggregations
-- MAGIC - Count positive mentions per user
-- MAGIC - Count negative mentions per user
-- MAGIC - Count total mentions (positive + negative only, exclude neutral)
-- MAGIC - Track timestamp range (min/max) per user
-- MAGIC - Filter out NULL mentions
-- MAGIC - Sort by total mentions (descending)
-- MAGIC
-- MAGIC ## Expected Output
-- MAGIC Materialized view: `gold_tweet_aggregations`
-- MAGIC Columns: mention, positive, negative, total, min_timestamp, max_timestamp
-- MAGIC
-- MAGIC ## Reference
-- MAGIC See Lab 0.1 Section 5 for aggregation patterns with FILTER clause

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task: Create Aggregation Materialized View
-- MAGIC
-- MAGIC TODO: Write SQL to create materialized view that:
-- MAGIC 1. Selects from tweets_gold table
-- MAGIC 2. Counts positive mentions: COUNT(*) FILTER (WHERE predicted_sentiment = 'positive')
-- MAGIC 3. Counts negative mentions: COUNT(*) FILTER (WHERE predicted_sentiment = 'negative')
-- MAGIC 4. Counts total mentions: COUNT(*) FILTER (WHERE predicted_sentiment IN ('positive', 'negative'))
-- MAGIC 5. Gets earliest timestamp: MIN(timestamp)
-- MAGIC 6. Gets latest timestamp: MAX(timestamp)
-- MAGIC 7. Filters out NULL mentions: WHERE mention IS NOT NULL
-- MAGIC 8. Groups by mention
-- MAGIC 9. Orders by total DESC

-- COMMAND ----------

-- TODO: Create materialized view with aggregations

-- Create Gold Aggregation Table
CREATE OR REFRESH LIVE TABLE tweets_gold_aggregated
COMMENT "Aggregated sentiment metrics per mention from tweets_gold"
AS
SELECT
    mention,

    -- Total number of mentions
    COUNT(*) AS mention_count,

    -- Average predicted sentiment score
    AVG(predicted_score) AS avg_predicted_score,

    -- Count of positive/neutral (1) predictions
    SUM(predicted_sentiment_id) AS positive_or_neutral_count,

    -- Count of negative predictions
    SUM(CASE WHEN predicted_sentiment_id = 0 THEN 1 ELSE 0 END) AS negative_count

FROM workspace.default.tweets_gold

-- Optional: exclude NULL mentions (recommended for cleaner analytics)
WHERE mention IS NOT NULL

GROUP BY mention

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Validation
-- MAGIC
-- MAGIC After pipeline execution, verify:
-- MAGIC - View exists: gold_tweet_aggregations
-- MAGIC - Sorted by total (highest first)
-- MAGIC - positive + negative = total (neutral excluded)
-- MAGIC - min_timestamp ≤ max_timestamp for all rows
-- MAGIC - No NULL mentions in output
