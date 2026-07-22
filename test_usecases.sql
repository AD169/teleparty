-- Aggregates ratings and title counts by start year and title type (silver join)
SELECT
    t.start_year,
    t.title_type,
    COUNT(*) AS total_titles,
    ROUND(AVG(r.average_rating), 2) AS avg_rating,
    MAX(r.num_votes) AS max_votes
FROM silver_titles t
JOIN silver_ratings r ON t.title_id = r.title_id
WHERE t.start_year IS NOT NULL
  AND t.start_year BETWEEN 1980 AND 2025
GROUP BY t.start_year, t.title_type
ORDER BY t.start_year DESC, total_titles DESC;

-- Highly-rated movies with substantial vote counts (gold; dedupe genre explode)
SELECT DISTINCT
    title_id,
    primary_title,
    start_year,
    average_rating,
    num_votes
FROM gold_category_wise_movies
WHERE title_type = 'movie'
  AND average_rating >= 8.5
  AND num_votes >= 100000
ORDER BY average_rating DESC, num_votes DESC
LIMIT 100;

-- Pattern matching on title names (gold)
SELECT
    title_type,
    COUNT(DISTINCT title_id) AS match_count,
    ROUND(AVG(average_rating), 2) AS avg_rating
FROM gold_category_wise_movies
WHERE LOWER(primary_title) LIKE '%star wars%'
   OR LOWER(primary_title) LIKE '%lord of the rings%'
GROUP BY title_type;

-- Top 3 highest-rated titles per year (gold; title-grain CTE)
WITH titles AS (
    SELECT DISTINCT
        title_id,
        primary_title,
        start_year,
        average_rating,
        num_votes
    FROM gold_category_wise_movies
    WHERE title_type = 'movie'
      AND num_votes > 50000
),
ranked_titles AS (
    SELECT
        primary_title,
        start_year,
        average_rating,
        num_votes,
        DENSE_RANK() OVER (
            PARTITION BY start_year
            ORDER BY average_rating DESC, num_votes DESC
        ) AS rank_in_year
    FROM titles
)
SELECT *
FROM ranked_titles
WHERE rank_in_year <= 3
ORDER BY start_year DESC, rank_in_year ASC;


----- pyspark test results
-- Q1_Aggregation: 8805.62 ms (Rows returned: 460)
-- Q2_Filtering: 1007.27 ms (Rows returned: 56)
-- Q3_StringSearch: 9201.55 ms (Rows returned: 10)
-- Q4_WindowRanking: 1175.02 ms (Rows returned: 281)

----- pinot test results
