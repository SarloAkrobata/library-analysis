from pyspark.sql.functions import col, to_date, datediff, when, year, current_date, min, max

def preprocess_data(dataframes):
    """
    Preprocess loaded dataframes for analysis.
    Args:
        dataframes (dict): Dictionary with table names and their corresponding DataFrames.
    Returns:
        DataFrame: Merged and preprocessed DataFrame.
    """
    # Rename columns to avoid conflicts
    dataframes['books'] = dataframes['books'].withColumnRenamed("title", "book_title")
    dataframes['libraries'] = dataframes['libraries'].withColumnRenamed("name", "library_name") \
                                                     .withColumnRenamed("city", "library_city")
    dataframes['customers'] = dataframes['customers'].withColumnRenamed("name", "customer_name") \
                                                     .withColumnRenamed("city", "customer_city")

    # Merge DataFrames
    merged_df = dataframes['checkouts'] \
        .join(dataframes['customers'], dataframes['checkouts']["patron_id"] == dataframes['customers']["id"], "left") \
        .join(dataframes['libraries'], dataframes['checkouts']["library_id"] == dataframes['libraries']["id"], "left") \
        .join(dataframes['books'], dataframes['checkouts']["id"] == dataframes['books']["id"], "left")

    # Convert dates and calculate days to return
    merged_df = merged_df.withColumn("checkout_date", to_date(col("date_checkout"), "yyyy-MM-dd")) \
                         .withColumn("return_date", to_date(col("date_returned"), "yyyy-MM-dd")) \
                         .withColumn("days_to_return", datediff(col("return_date"), col("checkout_date")))

    # Remove invalid records for days_to_return (negative or excessively high)
    merged_df = merged_df.filter((col("days_to_return") >= 0) & (col("days_to_return") <= 1826))

    # Normalize days_to_return column
    days_stats = merged_df.agg(min("days_to_return").alias("min_days"), max("days_to_return").alias("max_days")).collect()
    min_days, max_days = days_stats[0]["min_days"], days_stats[0]["max_days"]
    merged_df = merged_df.withColumn("normalized_days_to_return", (col("days_to_return") - min_days) / (max_days - min_days))

    # Create late return flag
    merged_df = merged_df.withColumn("late_return", when(col("days_to_return") > 28, 1).otherwise(0))

    # Add age column
    merged_df = merged_df.withColumn("age", year(current_date()) - year(to_date(col("birth_date"), "yyyy-MM-dd")))

    # Remove records with invalid age (less than 0 or greater than 100)
    merged_df = merged_df.filter((col("age") >= 0) & (col("age") <= 100))

    # Normalize age column
    age_stats = merged_df.agg(min("age").alias("min_age"), max("age").alias("max_age")).collect()
    min_age, max_age = age_stats[0]["min_age"], age_stats[0]["max_age"]
    merged_df = merged_df.withColumn("normalized_age", (col("age") - min_age) / (max_age - min_age))

    # Add age group column
    merged_df = merged_df.withColumn("age_group", when(col("age") <= 18, "Under 18")
                                     .when((col("age") > 18) & (col("age") <= 30), "18-30")
                                     .when((col("age") > 30) & (col("age") <= 50), "31-50")
                                     .when((col("age") > 50) & (col("age") <= 65), "51-65")
                                     .otherwise("66+"))
    return merged_df
