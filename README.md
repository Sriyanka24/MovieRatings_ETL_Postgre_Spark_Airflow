# ETL Pipeline with Spark and Airflow

This repository contains code for an ETL pipeline using Apache Spark and Apache Airflow. The pipeline performs the following tasks:

1. **Extracts** data from PostgreSQL tables.
2. **Transforms** the data by calculating average ratings.
3. **Loads** the transformed data back into a PostgreSQL database.
4. **Schedules** the ETL job using Apache Airflow.

## Project Structure

- `transformation.py`: Contains the Spark code for ETL operations.
  - **Extract**: Functions to load data from PostgreSQL tables into Spark DataFrames.
  - **Transform**: Aggregates the average ratings per movie.
  - **Load**: Writes the transformed data back to PostgreSQL.
  
- `airflow_dag.py`: Contains the Apache Airflow DAG definition.
  - Defines an ETL function.
  - Configures a DAG to schedule the ETL job.
  - Uses the `PythonOperator` to run the ETL function.

## Setup

### Prerequisites

- **Apache Spark**: Ensure Spark is installed and properly configured.
- **PostgreSQL**: Ensure PostgreSQL is running and accessible.
- **Apache Airflow**: Ensure Airflow is installed.

### Configuration

1. **Spark Configuration**:
   - Update the `spark.driver.extraClassPath` in `transformation.py` to point to your PostgreSQL JDBC driver.

2. **PostgreSQL Configuration**:
   - Update the `url`, `user`, and `password` options in the `transformation.py` file to match your PostgreSQL setup.

3. **Airflow Configuration**:
   - Place `airflow_dag.py` in your Airflow DAGs folder.
   - Update Airflow connection settings if necessary.

## Running the ETL Pipeline

### Local Execution

You can test the ETL functions locally by running the `transformation.py` script:

```bash
python transformation.py
