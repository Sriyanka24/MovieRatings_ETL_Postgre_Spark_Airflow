import pyspark
import pyspark.sql

## create spark session
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Python Spark example") \
    .config('spark.driver.extraClassPath', r"C:\Users\sriya\postgresql-42.7.3.jar") \
    .getOrCreate()

# read movies table from db using jdbc driver
movies_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
    .option("dbtable", "movies") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# read movies table from db using jdbc driver
users_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Transforming tables
# Aggregate the data 
avg_rating = users_df.groupBy("movie_id").mean("rating")

# Join the movies_df and avg_rating table on id
df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)



# Print all tables/dataframes
print(movies_df.show())
print(users_df.show())
print(avg_rating.show())
print(df.show())