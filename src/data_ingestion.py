from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

from config import OUTPUT_DATA_PATH


class DataIngestor(ABC):
    """
    Abstract class for data ingestion tasks.
    Defines a template for saving Spark DataFrames in different formats.
    """

    @abstractmethod
    def save_data(self, df: DataFrame):
        """Abstract method to save a Spark DataFrame."""
        pass


class CSVIngestor(DataIngestor):
    def save_data(self, df: DataFrame):
        selected_columns = ['patron_id', 'library_id', 'date_checkout', 'date_returned', 'customer_name',
                            'customer_city', 'state', 'zipcode', 'birth_date', 'gender',
                            'education', 'occupation', 'library_name', 'book_title',
                            'authors', 'publisher', 'publishedDate', 'categories',
                            'price', 'pages', 'checkout_date', 'days_to_return',
                            'late_return', 'age', 'age_group']
        cleaned_df = df.select(*selected_columns)

        cleaned_df.write.csv(OUTPUT_DATA_PATH, header=True, mode="overwrite")



class BigQueryIngestor(DataIngestor):
    def save_data(self, df: DataFrame):
        print(f"Ingesting Spark DataFrame into BigQuery table")

