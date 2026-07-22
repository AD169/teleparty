# benchmark_pinot.py
import requests
import json
import time

PINOT_BROKER_URL = "http://localhost:8099/query/sql"  # Default Pinot Query endpoint

queries = {
    "Q1_Aggregation": """
        SELECT
            t.start_year,
            t.title_type,
            COUNT(*) AS total_titles,
            AVG(r.average_rating) AS avg_rating,
            MAX(r.num_votes) AS max_votes
        FROM silver_titles t
        JOIN silver_ratings r ON t.title_id = r.title_id
        WHERE t.start_year IS NOT NULL
          AND t.start_year BETWEEN 1980 AND 2025
        GROUP BY t.start_year, t.title_type
        ORDER BY t.start_year DESC, total_titles DESC
        LIMIT 10000
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
            AVG(average_rating) AS avg_rating
        FROM gold_category_wise_movies
        WHERE LOWER(primary_title) LIKE '%star wars%'
           OR LOWER(primary_title) LIKE '%lord of the rings%'
        GROUP BY title_type
        LIMIT 1000
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
        LIMIT 10000
    """
}

headers = {'Content-Type': 'application/json'}

print("\n--- APACHE PINOT BENCHMARK RESULTS ---")
for name, sql in queries.items():
    payload = json.dumps({"sql": sql, "queryOptions": "explain=false"})

    # Warm-up run
    requests.post(PINOT_BROKER_URL, headers=headers, data=payload)

    # Timed run
    start_time = time.perf_counter()
    response = requests.post(PINOT_BROKER_URL, headers=headers, data=payload)
    end_time = time.perf_counter()

    client_side_time_ms = (end_time - start_time) * 1000

    if response.status_code == 200:
        data = response.json()
        server_exec_time_ms = data.get("timeUsedMs", 0)
        num_docs_scanned = data.get("numDocsScanned", 0)
        print(f"{name}: Server Time = {server_exec_time_ms} ms | Total E2E Time = {client_side_time_ms:.2f} ms (Docs Scanned: {num_docs_scanned})")
    else:
        print(f"{name}: FAILED (Status {response.status_code}) - {response.text}")
