from pyspark.sql import SparkSession

def create_spark_session(app_name="Library Late Returns Analysis"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data_csv(spark, file_paths):
    """
    Load multiple CSV files into Spark DataFrames.
    Args:
        spark (SparkSession): Active Spark session.
        file_paths (dict): Dictionary with keys as table names and values as file paths.
    Returns:
        dict: Dictionary with keys as table names and values as DataFrames.
    """
    dataframes = {}
    for table, path in file_paths.items():
        dataframes[table] = spark.read.csv(path, header=True, inferSchema=True)

    return dataframes
