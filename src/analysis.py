from pyspark.sql.functions import avg
import matplotlib.pyplot as plt

from config import OUTPUT_DATA_PATH


def analyze_data(merged_df):
    """
    Perform analysis on the preprocessed data.
    Args:
        merged_df (DataFrame): Preprocessed Spark DataFrame.
    Returns:
        dict: Analysis results as DataFrames for visualization.
    """
    results = {}

    # Late return rate by category
    results['late_by_category'] = merged_df.groupBy("categories").agg(avg("late_return").alias("late_return_rate"))

    # Late return rate by age group
    results['late_by_age_group'] = merged_df.groupBy("age_group").agg(avg("late_return").alias("late_return_rate"))

    # Late return rate by library location
    results['late_by_library_location'] = merged_df.groupBy("library_city").agg(avg("late_return").alias("late_return_rate"))

    return results

def visualize_analysis(results):
    """
    Visualize the analysis results.
    Args:
        results (dict): Analysis results as Spark DataFrames.
    """
    # Convert to Pandas for visualization
    late_by_category_pd = results['late_by_category'].orderBy("late_return_rate", ascending=False).toPandas()
    late_by_age_group_pd = results['late_by_age_group'].orderBy("late_return_rate", ascending=False).toPandas()
    late_by_library_location_pd = results['late_by_library_location'].orderBy("late_return_rate", ascending=False).toPandas()

    # Visualization for categories
    plt.figure(figsize=(10, 6))
    late_by_category_pd.head(5).plot(kind="bar", x="categories", y="late_return_rate", legend=False, color="orange", edgecolor="k")
    plt.title("Late Return Rate by Book Category (Top 5)")
    plt.xlabel("Book Category")
    plt.ylabel("Late Return Rate")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig(f"{OUTPUT_DATA_PATH}/late_return_rate_by_category.png", dpi=300, bbox_inches='tight')


    # Visualization for age groups
    plt.figure(figsize=(10, 6))
    late_by_age_group_pd.plot(kind="bar", x="age_group", y="late_return_rate", legend=False, color="skyblue", edgecolor="k")
    plt.title("Late Return Rate by Age Group")
    plt.xlabel("Age Group")
    plt.ylabel("Late Return Rate")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig(f"{OUTPUT_DATA_PATH}/late_return_rate_by_age_groups.png", dpi=300, bbox_inches='tight')

    # Visualization for library locations
    plt.figure(figsize=(10, 6))
    late_by_library_location_pd.head(5).plot(kind="bar", x="library_city", y="late_return_rate", legend=False, color="green", edgecolor="k")
    plt.title("Late Return Rate by Library Location (Top 5)")
    plt.xlabel("Library Location")
    plt.ylabel("Late Return Rate")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig(f"{OUTPUT_DATA_PATH}/late_return_rate_by_library.png", dpi=300, bbox_inches='tight')
