# benchmark_pyspark.py
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OLAP-Benchmark-PySpark") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load silver/gold layers
spark.read.parquet("/opt/spark/work-dir/silver/titles").createOrReplaceTempView("silver_titles")
spark.read.parquet("/opt/spark/work-dir/silver/ratings").createOrReplaceTempView("silver_ratings")
spark.read.parquet("/opt/spark/work-dir/gold/category_wise_movies").createOrReplaceTempView("gold_category_wise_movies")

queries = {
    "Q1_Aggregation": """
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
        ORDER BY t.start_year DESC, total_titles DESC
    """,
    "Q2_Filtering": """
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
        LIMIT 100
    """,
    "Q3_StringSearch": """
        SELECT
            title_type,
            COUNT(DISTINCT title_id) AS match_count,
            ROUND(AVG(average_rating), 2) AS avg_rating
        FROM gold_category_wise_movies
        WHERE LOWER(primary_title) LIKE '%star wars%'
           OR LOWER(primary_title) LIKE '%lord of the rings%'
        GROUP BY title_type
    """,
    "Q4_WindowRanking": """
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
        ORDER BY start_year DESC, rank_in_year ASC
    """
}

print("\n--- PYSPARK BENCHMARK RESULTS ---")
for name, sql in queries.items():
    # Warm-up run
    spark.sql(sql).collect()

    # Timed run
    start_time = time.perf_counter()
    res = spark.sql(sql).collect()
    end_time = time.perf_counter()

    execution_time_ms = (end_time - start_time) * 1000
    print(f"{name}: {execution_time_ms:.2f} ms (Rows returned: {len(res)})")

spark.stop()
