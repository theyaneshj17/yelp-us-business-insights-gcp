-- ================================================================
-- Yelp Gold Layer - BigQuery SQL Pipeline
-- Author: Theyaneshwaran Jayaprakash
-- Description: Creates all dimension, fact, bridge tables, and views
--              for the Yelp dataset gold layer in BigQuery.
-- Last Updated: 5/5/2025
-- ================================================================


-----------------------------------State Mapping Dimension-------------------------

CREATE OR REPLACE TABLE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_state_mapping` (
  state_id INT64,
  state STRING,
  state_name STRING,
  region_name STRING
);

INSERT INTO `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_state_mapping` (state_id, state, state_name, region_name)
SELECT
  ROW_NUMBER() OVER (ORDER BY state) AS state_id,
  state,
  state_name,
  region AS region_name
FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.state_mapping`
WHERE state IS NOT NULL AND state_name IS NOT NULL AND region IS NOT NULL;


-----------------------------Business Data Dimension------------------------------------------------

CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_business_data` (
  business_id STRING,        -- PK
  name STRING,
  city STRING,
  state STRING
);

CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_category` (
  category_id INT64,        
  category_name STRING       -- e.g., "Restaurants"
);

CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.bridge_business_category` (
  business_id STRING,        -- FK to dim_business_data
  category_id INT64
);



CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_category` AS
WITH distinct_categories AS (
  SELECT DISTINCT TRIM(LOWER(category)) AS category_name
  FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.business_data`,
       UNNEST(categories) AS category
  WHERE category IS NOT NULL
)
SELECT
  ROW_NUMBER() OVER (ORDER BY category_name) AS category_id,
  category_name
FROM distinct_categories;


CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.bridge_business_category` AS
SELECT
  b.business_id,
  c.category_id
FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.business_data` b,
     UNNEST(b.categories) AS category
JOIN `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_category` c
  ON TRIM(LOWER(category)) = c.category_name
WHERE b.business_id IS NOT NULL;


CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_business_data` AS
SELECT DISTINCT
  business_id,
  name,
  city,
  state
FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.business_data`
WHERE business_id IS NOT NULL;

-------------------------------User Dimension---------------------------------------

CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_user` (
  user_id STRING,
  name STRING,
  yelping_since DATE,
  elite STRING,
  friends STRING
);


INSERT INTO `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_user`
SELECT DISTINCT
  user_id,
  name,
  SAFE.PARSE_DATE('%Y-%m-%d', yelping_since) AS yelping_since,
  elite,
  friends
FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.Users`
WHERE user_id IS NOT NULL;



-----------------------------Business Fact-----------------------------------------


CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_business_data` (
  business_id STRING,       -- Foreign key to dim_business_data
  stars FLOAT64,            -- Average star rating
  review_count INT64,       -- Total number of reviews
  create_date DATE          -- Snapshot date of fact
);

INSERT INTO `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_business_data`
SELECT
  business_id,
  AVG(stars) AS stars,
  SUM(review_count) AS review_count,
  CURRENT_DATE() AS create_date
FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.business_data`
WHERE business_id IS NOT NULL
GROUP BY business_id;

-----------------------User Fact----------------------------

CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_user_metrics` (
  user_id STRING,                  -- FK to dim_user
  review_count INT64,
  average_stars FLOAT64,
  useful_votes INT64,
  funny_votes INT64,
  cool_votes INT64,
  fans INT64,
  compliment_total INT64,
  friends_count INT64,
  is_elite BOOL,
  elite_year_count INT64,
  create_date DATE
);

INSERT INTO `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_user_metrics`
SELECT
  user_id,
  review_count,
  average_stars,
  useful AS useful_votes,
  funny AS funny_votes,
  cool AS cool_votes,
  fans,

  -- Total compliments
  (
    COALESCE(compliment_hot, 0) + COALESCE(compliment_more, 0) + COALESCE(compliment_profile, 0) +
    COALESCE(compliment_cute, 0) + COALESCE(compliment_list, 0) + COALESCE(compliment_note, 0) +
    COALESCE(compliment_plain, 0) + COALESCE(compliment_cool, 0) + COALESCE(compliment_funny, 0) +
    COALESCE(compliment_writer, 0) + COALESCE(compliment_photos, 0)
  ) AS compliment_total,

  -- Friends count
  ARRAY_LENGTH(SPLIT(friends, ',')) AS friends_count,

  -- Elite metrics
  IFNULL(elite, '') != '' AS is_elite,
  ARRAY_LENGTH(SPLIT(elite, ',')) AS elite_year_count,

  CURRENT_DATE() AS create_date

FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.Users`
WHERE user_id IS NOT NULL;



select user_id, count(*) from `sp25-i535-thjaya-yelpanalysis.silver_layer.Users` group by user_id having count(*) > 1




-----------------Review_Fact-------------------------------------------------------
CREATE OR REPLACE TABLE `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_review` (
  review_id STRING,
  user_id STRING,            -- FK to dim_user
  business_id STRING,        -- FK to dim_business
  stars FLOAT64,
  useful INT64,
  funny INT64,
  cool INT64,
  review_text STRING,

  -- Date breakdowns
  review_date DATE,
  review_year INT64,
  review_yearmonth STRING,   -- Format: YYYY-MM
  create_date DATE
);

INSERT INTO `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_review`
SELECT
  review_id,
  user_id,
  business_id,
  stars,
  useful,
  funny,
  cool,
  text AS review_text,

-- Properly parsed from full timestamp string
  CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date) AS DATE) AS review_date,
  EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date)) AS review_year,
  FORMAT_TIMESTAMP('%Y-%m', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', date)) AS review_yearmonth,

  CURRENT_DATE() AS create_date



FROM `sp25-i535-thjaya-yelpanalysis.silver_layer.review_filtered`
WHERE review_id IS NOT NULL 
  AND user_id IS NOT NULL 
  AND business_id IS NOT NULL;




-----------------Aggregated Tables----------------------

CREATE OR REPLACE VIEW `sp25-i535-thjaya-yelpanalysis.gold_layer.vw_business_performance_summary` AS
SELECT
  b.business_id,
  b.name,
  b.city,
  b.state,
  COUNT(r.review_id) AS total_reviews,
  AVG(r.stars) AS avg_rating,
  SUM(r.useful) AS total_useful_votes
FROM `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_review` r
JOIN `sp25-i535-thjaya-yelpanalysis.gold_layer.dim_business_data` b
  ON r.business_id = b.business_id
GROUP BY b.business_id, b.name, b.city, b.state;


select * from `sp25-i535-thjaya-yelpanalysis.gold_layer.vw_business_performance_summary`

select business_id, count(*) from  `sp25-i535-thjaya-yelpanalysis.gold_layer.fact_review`
group by business_id having count(*) > 1





