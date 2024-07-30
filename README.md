# ETL Pipeline for Cardinal Surveying

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline to process student data for Cardinal Surveying. Cardinal Surveying is committed to identifying and hiring top talent efficiently. The ETL pipeline facilitates this by processing student registration data, extracting relevant information, transforming and normalizing the data, and finally loading it into an organized format for easy analysis and decision-making.

## Project Structure

```
ETL_Script_Cardinal_Surveying/
├── etl/
│   ├── __init__.py
│   ├── pipeline.py
│   ├── transformations.py
├── input/
│   └── Pipeline Class Registration_v3.csv
├── output/
│   └── normalized_data.xlsx
├── venv/
├── .gitignore
├── README.md
```

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/shashankkongara/ETL_Script_Cardinal_Surveying.git
   cd ETL_Script_Cardinal_Surveying
   ```

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required packages**:
   ```bash
   pip install pandas openpyxl
   ```

## Usage

1. **Place the input data file**:
   - Place your input CSV file in the `input` directory. Ensure the file is named `Pipeline Class Registration_v2.csv`.

2. **Run the ETL pipeline**:
   - Execute the `pipeline.py` script to run the ETL process.
   ```bash
   python etl/pipeline.py
   ```

3. **Check the output**:
   - The processed data will be saved in the `output` directory as `normalized_data.xlsx`. This file contains the following sheets:
     - `Student PII`
     - `Skills`
     - `Top Candidates`
     - `Student Social`

## Detailed Explanation

### `transformations.py`

This file contains the `DataTransformer` class, which handles the data transformations.

#### Methods:

1. **`__init__`**:
   - Initializes the transformer with the input data.

2. **`month_to_number`**:
   - Converts month names to their corresponding numerical values.

3. **`extract_profile_name`**:
   - Extracts LinkedIn profile names from URLs using regex.

4. **`transform_job_status`**:
   - Converts job status to binary format (1 for seeking jobs, 0 otherwise).

5. **`create_custom_student_id`**:
   - Generates a unique student ID using a hash of the student ID and birth month.

6. **`rename_columns`**:
   - Renames columns to standardize the input data.

7. **`perform_basic_transformations`**:
   - Applies basic transformations, such as converting emails to lowercase and creating custom student IDs.

8. **`map_experience_levels`**:
   - Maps textual experience levels to numerical values.

9. **`filter_job_seekers`**:
   - Filters data to include only students who are seeking jobs.

10. **`calculate_attendance`**:
    - Calculates the total sessions attended and percentage attendance.

11. **`get_transformed_data`**:
    - Returns the transformed data.

### `pipeline.py`

This script orchestrates the ETL process using the `DataTransformer` class.

#### Steps:

1. **Load new data**:
   - Reads the input CSV file.

2. **Initialize the transformer**:
   - Creates an instance of `DataTransformer` and performs transformations.

3. **Check for existing data**:
   - If existing data is found, it combines it with the new data.

4. **Sort and deduplicate**:
   - Sorts data by timestamp and removes duplicates.

5. **Save the output**:
   - Writes the processed data to an Excel file with multiple sheets.
