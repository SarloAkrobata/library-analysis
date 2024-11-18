import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.data_preprocessor import preprocess_data

class TestDataPreprocessor(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        self.mock_dataframes = {
            "books": self.spark.createDataFrame([Row(id="1", title="Book A", categories="Fiction")]),
            "checkouts": self.spark.createDataFrame([
                Row(id="1", patron_id="1", library_id="1", date_checkout="2024-01-01", date_returned="2024-02-01")
            ]),
            "customers": self.spark.createDataFrame([
                Row(id="1", name="Customer A", birth_date="1990-01-01", city="CityA")
            ]),
            "libraries": self.spark.createDataFrame([Row(id="1", name="Library A", city="CityA")])
        }

    def tearDown(self):
        self.spark.stop()

    def test_preprocess_data(self):
        merged_df = preprocess_data(self.mock_dataframes)

        # Validate column presence
        expected_columns = {"checkout_date", "return_date", "days_to_return", "late_return", "age_group"}
        self.assertTrue(expected_columns.issubset(set(merged_df.columns)))

        # Validate late_return calculation
        data = merged_df.select("late_return").collect()
        self.assertEqual(data[0]["late_return"], 1)  # Expecting late return because >28 days

if __name__ == "__main__":
    unittest.main()
