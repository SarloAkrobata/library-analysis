# Library Late Returns Analysis
## Interesting fact
It is very interesting that there is no user who borrowed book more than once.
## Project usage:
Place input data csv
```bash
data/input/
```
Output data will be in:
```bash
data/output/
```
### Build image

```bash
docker build --no-cache -t library_analysis .
```
### Create container


```bash
docker run -d --name library_analysis -v ~/library_analysis/output:/app/output library_analysis tail -f /dev/null
```

### Run program

```bash
docker exec library_analysis python main.py
```



### Documentation

---
### **Modules**

#### 1. `data_loader.py`
Handles data loading and Spark session creation.

##### Functions:
- **`create_spark_session(app_name: str)`**
  - Creates and returns a Spark session.
  - **Parameters:** `app_name` (str): Name for the Spark application.
  - **Returns:** `SparkSession`
  
- **`load_data_csv(spark: SparkSession, file_paths: dict)`**
  - Loads CSV files into Spark DataFrames.
  - **Parameters:** 
    - `spark` (SparkSession): Active Spark session.
    - `file_paths` (dict): Dictionary mapping table names to file paths.
  - **Returns:** `dict` with table names as keys and DataFrames as values.

---

#### 2. `data_preprocessor.py`
Processes raw data to prepare it for analysis.

##### Functions:
- **`preprocess_data(dataframes: dict)`**
  - Preprocesses and merges the input DataFrames.
  - Adds calculated columns like `days_to_return`, `late_return`, and `age_group`.
  - **Parameters:** `dataframes` (dict): Dictionary of input DataFrames.
  - **Returns:** Preprocessed and merged DataFrame.

---

#### 3. `analysis.py`
Performs data analysis and visualization.

##### Functions:
- **`analyze_data(merged_df: DataFrame)`**
  - Analyzes the preprocessed data.
  - Computes late return rates by book category, age group, and library location.
  - **Parameters:** `merged_df` (DataFrame): Preprocessed DataFrame.
  - **Returns:** `dict` containing analysis results as Spark DataFrames.

- **`visualize_analysis(results: dict)`**
  - Visualizes the analysis results using Matplotlib.
  - **Parameters:** `results` (dict): Analysis results as DataFrames.
  - **Returns:** Writes png chart images to output dir
---

#### 4. `main.py`
The entry point for the analysis.

##### Workflow:
1. Create a Spark session using `create_spark_session`.
2. Load data with `load_data`.
3. Preprocess data using `preprocess_data`.
4. Perform analysis with `analyze_data`.
5. Visualize results using `visualize_analysis`.

---

### **Dependencies**
- **Python Libraries:**
  - `pyspark`: For data processing and analysis.
  - `matplotlib`: For visualizations.
- **Data Files:** Provide CSV files for `books`, `checkouts`, `customers`, and `libraries`.

---

### **Testing**
Unit tests ensure functionality across all modules. The test files are:
- `test_data_loader.py`: Tests data loading and Spark session creation.
- `test_data_preprocessor.py`: Validates preprocessing logic.
- `test_analysis.py`: Ensures analysis outputs are accurate.

Run tests using:
```bash
python -m unittest discover
```

---

### **Expected Outputs**
1. Analysis results as:
   - Late return rates by book category.
   - Late return rates by user age group.
   - Late return rates by library location.
2. Visualizations:
   - Bar charts for the top late-returning categories, age groups, and library locations.


### **Conclusion**
- The youngest (<18) and the older (51-65) users have higher late return rates
- Certain categories, such as Agriculture and Banking law, had higher late return rates.

### Recommendations to the Library:
1. Proactive Notifications:
   - Send automated reminders (email/SMS) to users before the due date, especially for high-risk categories like Agriculture or younger users.
2. Flexible Loan Policies:
   - Extend loan periods for specific book categories (e.g., lengthy books or popular genres).
   - Allow personalized loan durations based on a user's borrowing history.