import unittest
from pyspark.sql import SparkSession
from src.data_loader import create_spark_session, load_data_csv

class TestDataLoader(unittest.TestCase):
    def setUp(self):
        self.spark = create_spark_session()

    def tearDown(self):
        self.spark.stop()

    def test_create_spark_session(self):
        self.assertIsInstance(self.spark, SparkSession)

    def test_load_data(self):
        # Mock file paths
        mock_file_paths = {
            "mock_table": "path/to/mock.csv"  # Use actual or dummy paths for tests
        }

        # Expecting empty results since paths don't exist
        dataframes = load_data_csv(self.spark, mock_file_paths)
        self.assertIn("mock_table", dataframes)
        self.assertTrue(dataframes["mock_table"].isEmpty())

if __name__ == "__main__":
    unittest.main()
