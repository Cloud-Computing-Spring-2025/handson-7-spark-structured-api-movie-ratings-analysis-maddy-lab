from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Movie_Watching_Trends"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    schema = """
        UserID INT, MovieID INT, MovieTitle STRING, Genre STRING, Rating FLOAT, ReviewCount INT, 
        WatchedYear INT, UserLocation STRING, AgeGroup STRING, StreamingPlatform STRING, 
        WatchTime INT, IsBingeWatched BOOLEAN, SubscriptionStatus STRING
    """
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def analyze_movie_trends(df):
    """
    Analyze movie watching trends over the years.
    """
    # Group by WatchedYear and count the number of movies watched in each year
    movie_watching_trends = df.groupBy("WatchedYear").agg(count("*").alias("MoviesWatched"))

    # Sort the data by the number of movies watched
    movie_watching_trends = movie_watching_trends.sort(col("MoviesWatched").desc())

    return movie_watching_trends

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 3.
    """
    spark = initialize_spark()

    input_file = "input/movie_ratings_data.csv"
    output_file = "Outputs/movie_watching_trends.csv"

    df = load_data(spark, input_file)
    result_df = analyze_movie_trends(df)
    write_output(result_df, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
