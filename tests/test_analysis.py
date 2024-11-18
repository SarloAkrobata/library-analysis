import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.analysis import analyze_data

class TestAnalysis(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        self.mock_df = self.spark.createDataFrame([
            Row(categories="Fiction", late_return=1, age_group="18-30", library_city="CityA"),
            Row(categories="Fiction", late_return=0, age_group="18-30", library_city="CityA"),
            Row(categories="Non-Fiction", late_return=1, age_group="31-50", library_city="CityB"),
        ])

    def tearDown(self):
        self.spark.stop()

    def test_analyze_data(self):
        results = analyze_data(self.mock_df)

        # Validate late return rate by category
        late_by_category = results["late_by_category"]
        late_by_category_data = late_by_category.filter(late_by_category.categories == "Fiction").collect()
        self.assertAlmostEqual(late_by_category_data[0]["late_return_rate"], 0.5)

        # Validate late return rate by age group
        late_by_age_group = results["late_by_age_group"]
        late_by_age_group_data = late_by_age_group.filter(late_by_age_group.age_group == "18-30").collect()
        self.assertAlmostEqual(late_by_age_group_data[0]["late_return_rate"], 0.5)

if __name__ == "__main__":
    unittest.main()
