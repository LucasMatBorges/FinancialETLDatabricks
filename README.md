# Databricks ETL Pipeline Project

## Overview

This project demonstrates an ETL (Extract, Transform, Load) pipeline implemented using Databricks and Delta Lake. The pipeline processes transactional financial data from multiple sources, applies transformations, and stores the final data in Delta Tables. It includes data quality checks and ensures that the data is partitioned appropriately for optimized querying.

## Project Structure

- **Notebooks**: Contains Databricks notebooks for each stage of the ETL pipeline.
- **Modules**: Python scripts defining functions used in the ETL pipeline.
- **Data**: Sample data files in Parquet and CSV format.
- **Tests**: Unit tests for the functions used in the pipeline.

## Data Sources

### Source Data
- **Source1**: Parquet file containing financial data.
- **source2**: CSV file containing financial data.

### Lookup Data
- **Client_Secured_Ind**: CSV file containing client secured information.
- **exchange_rates**: CSV file containing exchange rates.

## ETL Pipeline

### Steps

1. **Data Extraction**: Read the source data files into Spark DataFrames.
2. **Data Transformation**: Apply various transformations to clean and enrich the data.
   - Remove leading zeros from client numbers.
   - Convert values to appropriate data types.
   - Merge lookup data.
   - Calculate new columns like `AmountEUR`, `ClientSecuredIND`, `EnterpriseSize`, and `SecuredAmountEUR`.
3. **Data Quality Checks**: Implement quality checks to ensure data integrity.
4. **Data Load**: Write the final transformed data to Delta Tables in the Databricks Catalog.

### Functions

- **remove_leading_zeros(client_number)**: Removes leading zeros from client numbers.
- **convert_to_yn(value)**: Converts 'yes'/'no' values to 'Y'/'N'.
- **to_int(val)**: Converts a value to integer.
- **to_decimal(val)**: Converts a value to decimal.
- **to_date(value)**: Converts a string to a date.
- **calculate_amount_eur(original_amount, original_currency, exchange_rate)**: Calculates the amount in EUR.
- **yn_to_bool(val)**: Converts 'Y'/'N' to boolean.
- **determine_enterprise_size(num_employees)**: Determines the enterprise size based on the number of employees.
- **calculate_secured_amount(df)**: Calculates the secured amount for each client.

## Setup

### Prerequisites

- Databricks account
- Databricks CLI installed and configured
- Python 3.x
- Git

### Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/yourusername/your-repo.git
   cd your-repo
   
### Running the ETL Pipeline
1. **Upload data files to DBFS**
databricks fs cp local-path-to-your-data dbfs:/FileStore/shared_uploads/yourusername/ -r

2. **Run the notebooks in Databricks:**

- Open Databricks workspace.
- Import the notebooks from the notebooks directory.
- Run the notebooks sequentially to execute the ETL pipeline.

### Data Quality Checks
The pipeline includes several quality checks:
 - Data type validation.
 - Range checks for numerical values.
 - Null checks for critical columns.
 
### Monitoring and Logs
The execution logs and data quality check results can be accessed from the Databricks workspace:

- Go to Jobs > Delta Live Tables to see the pipeline runs.
- Check the Logs tab for detailed logs and error messages.
