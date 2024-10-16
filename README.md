# Databricks ETL Pipeline Project!!

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

## Setup

### Installation
1. **Data Ingestion of the Data Files**
   Make the Ingestion of the Data Files in the Catalog, creating a Table for each of the files: `Source1`, `Source2`, `Exchanges_Rates` and `Client_Secured_Ind`;  

                                 Data Ingestion > Create or Modify table > Paste the File > Give a Table > Create Table 

  <div align="center"">
  <img src="https://github.com/LucasMatBorges/FinancialETLDatabricks/assets/11663404/45626f15-6c48-4e4a-a46f-6c0eaefb96bf" alt="Descrição da Imagem" width="800"/>
  </div>

2. **Upload of the Notebook and modules on Databricks Workspace**  
   Make the upload of the files all in the same folder:

  <div align="center"">
  <img src="https://github.com/LucasMatBorges/FinancialETLDatabricks/assets/11663404/720ad514-043b-4ca5-98ef-47ee1ed3a45a" alt="Descrição da Imagem" width="800"/>
  </div>

4. **Install the Pytest in the Compute Engine**
   Inside the Compute Menu, select the desired Cluster and go to Libraries:
   
                                                      Install new > PyPi > Package > pytest
   
![image](https://github.com/LucasMatBorges/FinancialETLDatabricks/assets/11663404/be1c5096-e0ae-43cd-97f6-ff32b56834f5)
   
### Running the ETL Pipeline
1. **Upload data files to DBFS**

                              databricks fs cp local-path-to-your-data dbfs:/FileStore/shared_uploads/yourusername/ -r

3. **Run the notebooks in Databricks:**
  - Run the Notebook 'Solution ABN AMRO'
  This is going to make all the cleansing and transformation until finally generate the final table 'financialtransactions' on the Unity Catalog
  In the end of the Notebook there's the answer for the questions of the case

### Unit Testing
   All the modules created and used in the ETL Process were tested by a separeted Notebook
   - Run the Notebook 'Unit Testing with Pytest'
  
<div align="center"">
  <img src="https://github.com/LucasMatBorges/FinancialETLDatabricks/assets/11663404/f5d79ca3-1985-4812-895b-7baf7988fa9c" alt="Descrição da Imagem" width="600"/>
</div>

### Data Quality Checks
   - Create a Delta Live Table Pipeline and Attach the Notebook 'Data Quality Check'  
   The pipeline includes several quality checks:  
    - Data type validation.  
    - Range checks for numerical values.  
    - Null checks for critical columns.  
 
### Monitoring and Logs
   The execution logs and data quality check results can be accessed from the Databricks workspace:  
   
   After Run the Pipeline Check, it's possible to visualize the Data Quality  
   - Data Engineering > Delta Live Tables to see the pipeline runs.  
   - Check the Logs tab for detailed logs and error messages.  

![image](https://github.com/LucasMatBorges/FinancialETLDatabricks/assets/11663404/64bcfa0d-8c63-4ea5-b666-8ddb21121962)
