from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
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

def detect_churn_risk(df):
    """
    Identify users at risk of churn (Canceled subscription and low watch time).
    """
    churn_risk_users = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    
    churn_risk_count = churn_risk_users.count()
    total_users_count = df.count()
    
    return churn_risk_count, total_users_count

def write_output(churn_risk_count, output_path):
    """
    Write the result in the required format to a CSV file.
    """
    with open(output_path, 'w') as f:
        f.write("Churn Risk Users,Total Users\n")
        f.write(f"Users with low watch time & canceled subscriptions,{churn_risk_count}\n")

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()

    input_file = "input/movie_ratings_data.csv"
    output_file = "Outputs/churn_risk_users.csv"

    df = load_data(spark, input_file)
    churn_risk_count, total_users_count = detect_churn_risk(df)
    write_output(churn_risk_count, output_file)

    spark.stop()

if __name__ == "__main__":
    main()
