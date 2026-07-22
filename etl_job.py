from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import os

# Absolute path so Spark executors on the worker resolve the same bind-mounted files
BASE_PATH = "/opt/spark/work-dir"

spark = SparkSession.builder \
    .appName("Optimized_Production_Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "80") \
    .getOrCreate() # Reuses an existing session if active to prevent memory conflicts

spark.conf.set("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution

## table configurations for transformation
transform_config = {
    "name": {
        "columns": ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"],
        "data_types": {"nconst": "string", "primaryName": "string", "birthYear": "int", "deathYear": "int", "primaryProfession": "string", "knownForTitles": "string"},
        "rename": {"nconst": "name_id", "primaryName": "primary_name", "birthYear": "birth_year", "deathYear": "death_year", "primaryProfession": "primary_profession", "knownForTitles": "known_for_titles"},
        "sorted_by": ["name_id"]
    },
    "principals": {
        "columns": ["tconst", "ordering", "nconst", "category", "job", "characters"],
        "data_types": {"tconst": "string", "ordering": "int", "nconst": "string", "category": "string", "job": "string", "characters": "string"},
        "rename": {"tconst": "title_id", "ordering": "ordering", "nconst": "name_id", "category": "category", "job": "job", "characters": "characters"},
        "sorted_by": ["title_id", "name_id"]
    },
    "akas": {
        "columns": ["titleId", "ordering", "title", "region", "language", "types", "attributes", "isOriginalTitle"],
        "data_types": {"titleId": "string", "ordering": "int", "title": "string", "region": "string", "language": "string", "types": "string", "attributes": "string", "isOriginalTitle": "boolean"},
        "rename": {"titleId": "title_id", "ordering": "ordering", "title": "title", "region": "region", "language": "language", "types": "types", "attributes": "attributes", "isOriginalTitle": "is_original_title"},
        "sorted_by": ["title_id", "region"]
    },
    "titles": {
        "columns": ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"],
        "data_types": {"tconst": "string", "titleType": "string", "primaryTitle": "string", "originalTitle": "string", "isAdult": "boolean", "startYear": "int", "endYear": "int", "runtimeMinutes": "int", "genres": "string"},
        "rename": {"tconst": "title_id", "titleType": "title_type", "primaryTitle": "primary_title", "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "start_year", "endYear": "end_year", "runtimeMinutes": "runtime_minutes", "genres": "genres"},
        "sorted_by": ["title_id","title_type","start_year","end_year"]
    },
    "ratings": {
        "columns": ["tconst", "averageRating", "numVotes"],
        "data_types": {"tconst": "string", "averageRating": "float", "numVotes": "int"},
        "rename": {"tconst": "title_id", "averageRating": "average_rating", "numVotes": "num_votes"},
        "sorted_by": ["title_id"]
    }
}

## def for transforming the dataframe based on the config
def transform_dataframe(df, config):
    # Select only the specified columns
    df = df.select(*config["columns"])
    
    # Cast columns; IMDb \N nulls are handled at read via nullValue
    for column, data_type in config["data_types"].items():
        df = df.withColumn(column, expr(f"TRY_CAST({column} AS {data_type})"))
    
    # Rename columns as specified
    for old_name, new_name in config["rename"].items():
        df = df.withColumnRenamed(old_name, new_name)
    
    if "genres" in config["columns"]:
        df = df.withColumn("genres", expr("""replace(replace(replace(replace(genres,'[',''),']',''),'"',''),' ','')"""))
    if "characters" in config["columns"]:
        df = df.withColumn("characters", expr("""replace(replace(replace(characters,'[',''),']',''),'"','')"""))

    return df

def write_to_parquet(df, output_path, partition_by=None, sorted_by=None):
    # Create the output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    partition_cols = partition_by if isinstance(partition_by, list) else ([partition_by] if partition_by else [])
    sort_cols = sorted_by if isinstance(sorted_by, list) else ([sorted_by] if sorted_by else [])

    # Write the DataFrame to Parquet format
    if partition_cols and sort_cols:
        df = df.repartition(*partition_cols).sortWithinPartitions(*sort_cols)
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(output_path)
    elif partition_cols:
        df = df.repartition(*partition_cols)
        df.write.mode("overwrite").partitionBy(*partition_cols).parquet(output_path)
    elif sort_cols:
        # Prefer sort-within-partitions over a global sort to avoid OOM on multi-GB IMDb dumps
        df = df.repartition(8).sortWithinPartitions(*sort_cols)
        df.write.mode("overwrite").parquet(output_path)
    else:
        df.write.mode("overwrite").parquet(output_path)


## write to parquet files - considering this as silver directly as we don't need for bronze layer as per the current requirement
_csv_opts = dict(header=True, inferSchema=True, sep="\t", nullValue="\\N")
df_names = spark.read.csv(f"{BASE_PATH}/name.basics.tsv", **_csv_opts)
df_principals = spark.read.csv(f"{BASE_PATH}/title.principals.tsv", **_csv_opts)
df_akas = spark.read.csv(f"{BASE_PATH}/title.akas.tsv", **_csv_opts)
df_titles = spark.read.csv(f"{BASE_PATH}/title.basics.tsv", **_csv_opts)
df_ratings = spark.read.csv(f"{BASE_PATH}/title.ratings.tsv", **_csv_opts)

df_names = transform_dataframe(df_names, transform_config["name"])
df_principals = transform_dataframe(df_principals, transform_config["principals"])
df_akas = transform_dataframe(df_akas, transform_config["akas"])
df_titles = transform_dataframe(df_titles, transform_config["titles"])
df_ratings = transform_dataframe(df_ratings, transform_config["ratings"])

write_to_parquet(df_names, f"{BASE_PATH}/silver/names", partition_by=transform_config["name"].get("partition_by"), sorted_by=transform_config["name"].get("sorted_by"))
write_to_parquet(df_principals, f"{BASE_PATH}/silver/principals", partition_by=transform_config["principals"].get("partition_by"), sorted_by=transform_config["principals"].get("sorted_by"))
write_to_parquet(df_akas, f"{BASE_PATH}/silver/akas", partition_by=transform_config["akas"].get("partition_by"), sorted_by=transform_config["akas"].get("sorted_by"))
write_to_parquet(df_titles, f"{BASE_PATH}/silver/titles", partition_by=transform_config["titles"].get("partition_by"), sorted_by=transform_config["titles"].get("sorted_by"))
write_to_parquet(df_ratings, f"{BASE_PATH}/silver/ratings", partition_by=transform_config["ratings"].get("partition_by"), sorted_by=transform_config["ratings"].get("sorted_by"))


## Gold layer of data for consumption -
# this is the final layer of data that will be used for analytics and reporting.
# This layer is optimized for query performance and is typically denormalized to reduce the number of joins required for analysis.
# The gold layer can be stored in a variety of formats, including Parquet, ORC, or Delta Lake, depending on the specific requirements of the use case.


# Usecase 1 -  Type and Genre wise movies over the years, best rated, actors, etc. - this can be used for recommendation engine, analytics, etc.

df_names = spark.read.parquet(f"{BASE_PATH}/silver/names")
df_principals = spark.read.parquet(f"{BASE_PATH}/silver/principals")

df_ratings = (spark.read.parquet(f"{BASE_PATH}/silver/ratings")
            .withColumn("overall_avg", expr("avg(average_rating) over()"))
            .withColumn("total_votes", expr("sum(num_votes) over()"))
            .withColumn("weighted_rating", expr("((num_votes/(num_votes + 1000)) * average_rating) + ((1000/(num_votes + 1000)) * overall_avg)")) ## based on IMDB weighted rating formula
            .select("title_id", "average_rating", "num_votes", "weighted_rating"))

df_principals_collected = (df_principals
                 .join(df_names, "name_id", "left")
                 .groupBy("title_id")
                 .agg(
                    expr("concat_ws(',', collect_list(case when category in ('actor', 'actress') then primary_name end)) as actors"),
                    expr("concat_ws(',', collect_list(case when category in ('director') then primary_name end)) as directors"),
                    expr("concat_ws(',', collect_list(case when category in ('producer') then primary_name end)) as producers")
                      )
                 )

df_titles = spark.read.parquet(f"{BASE_PATH}/silver/titles")
df_akas = spark.read.parquet(f"{BASE_PATH}/silver/akas").withColumn("region", expr("coalesce(region, 'Unknown')"))

df_reg_agg = df_akas.where("region != 'Unknown'").groupBy("title_id").agg(expr("concat_ws(',', collect_list(region)) as regions"), expr("count(distinct region) as region_count"))


df_genre = (df_titles.join(df_ratings, "title_id", "left")
           .join(df_principals_collected, "title_id", "left")
           .join(df_reg_agg, "title_id", "left")
           .withColumn("genres", expr("explode_outer(split(genres, ','))"))
           .withColumn("genres", expr("coalesce(trim(genres), 'unknown')"))
            )

df_genre.repartition("title_type", "genres").sortWithinPartitions("weighted_rating", ascending=False).write.mode("overwrite").partitionBy("title_type", "genres").parquet(f"{BASE_PATH}/gold/category_wise_movies")

# usecase 2 - Top rated actors, directors, producers, etc.
df_top_cast = (df_principals.join(df_ratings, "title_id", "left")
            .join(df_names, "name_id", "left")
            .groupBy("name_id", "primary_name", "category", "birth_year", "death_year").agg(
                expr("avg(weighted_rating) as avg_weighted_rating"),
                expr("count(weighted_rating) as total_movies")
            )
            .withColumn("rank", expr("dense_rank() over (partition by category order by avg_weighted_rating desc)"))
            )

df_top_cast.repartition("category").sortWithinPartitions("avg_weighted_rating", ascending=False).write.mode("overwrite").partitionBy("category").parquet(f"{BASE_PATH}/gold/top_category_wise_cast")

spark.stop()