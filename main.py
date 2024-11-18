from config import file_paths
from src.data_ingestion import CSVIngestor
from src.data_loader import create_spark_session, load_data_csv
from src.data_preprocessor import preprocess_data
from src.analysis import analyze_data, visualize_analysis

def main():
    # Create Spark session and load data
    spark = create_spark_session()
    dataframes = load_data_csv(spark, file_paths)

    # Preprocess data
    merged_df = preprocess_data(dataframes)
    # Ingest data
    csv_ingestor = CSVIngestor()
    csv_ingestor.save_data(merged_df)
    # Analyze data
    analysis_results = analyze_data(merged_df)

    # Visualize results
    visualize_analysis(analysis_results)

if __name__ == "__main__":
    main()
