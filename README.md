# Food Delivery Pipeline 🚴‍♂️🍔

This project is a complete ETL pipeline built around a food delivery startup use case, featuring automated data ingestion, transformation, and star schema modeling using Python, PostgreSQL, and Apache Airflow orchestration.

## 📌 Features
- **Complete ETL Pipeline**: Fully modular scripts for extraction, transformation, and loading
- **Apache Airflow Integration**: Automated workflow orchestration with DAG scheduling
- **Data Cleaning & Validation**: Comprehensive data cleaning with error handling and logging
- **Star Schema Implementation**: Production-ready dimensional modeling with fact and dimension tables
- **PostgreSQL Integration**: Full database connection and schema management
- **Docker Environment Setup**: Multi-service environment with PostgreSQL, PgAdmin, and Airflow using Docker Compose
- **Error Handling & Logging**: Comprehensive logging and exception handling throughout the pipeline

## 📁 Project Structure
```
├── dags/                    # Apache Airflow DAGs
│   └── food_delivery_etl_dag.py  # Main ETL pipeline DAG
├── scripts/                 # ETL pipeline modules
│   ├── ETL.py              # Main ETL orchestrator with table management
│   ├── extract.py          # Data extraction and database loading
│   ├── transform.py        # Data transformation, cleaning & star schema creation
│   └── load.py             # Dimension and fact table population
├── data/
│   └── source/             # Raw data files
│       └── Deliveries.csv  # Food delivery dataset
├── Diagrams/               # Architecture and schema diagrams
│   ├── DAG.png            # Airflow DAG visualization
│   └── Star_schema.png    # Data warehouse schema
├── env/                    # Python virtual environment
├── docker-compose.yml      # Docker Compose environment configuration
└── requirements.txt        # Python dependencies
```

## 📊 Dataset

This pipeline processes the **Zomato Delivery Operations Analytics Dataset** from Kaggle, which contains comprehensive food delivery transaction data including delivery metrics, location coordinates, time stamps, and operational details.

**Dataset Source**: [Zomato Delivery Operations Analytics Dataset](https://www.kaggle.com/datasets/saurabhbadole/zomato-delivery-operations-analytics-dataset)

### Data Schema
The raw dataset includes fields such as:
- Delivery person information (ID, age, ratings)
- Order details (date, time, type, multiple deliveries)
- Location data (restaurant and delivery coordinates)
- Operational metrics (time taken, weather, traffic density)
- Vehicle and festival information

## 🗄️ Star Schema
![Star Schema](Diagrams/Star_schema.png)

The pipeline transforms the raw data into the following dimensional model:
- **Fact Table**: `fact_deliveries` (delivery transactions)
- **Dimension Tables**: 
  - `dim_delivery_person` (delivery personnel details)
  - `dim_location` (restaurant and delivery locations)
  - `dim_datetime` (time dimensions)
  - `dim_vehicle` (vehicle information)

## 🔧 Tech Stack
- **Orchestration**: Apache Airflow (DAG scheduling, task management, monitoring)
- **Data Processing**: Python, Pandas, SQLAlchemy, NumPy
- **Database**: PostgreSQL 13 with dual schema architecture (raw_data + star_schema)
- **Database Management**: PgAdmin 4 for administration and monitoring
- **Environment Setup**: Docker Compose for multi-service environment orchestration
- **Environment Management**: Python virtual environment with pip
- **Development & Analysis**: Jupyter Notebook for interactive data exploration
- **Configuration**: python-dotenv for environment variable management
- **Logging**: Python logging module for comprehensive pipeline monitoring

## 🚀 Pipeline Architecture

### Airflow DAG Workflow
![Airflow DAG](Diagrams/DAG.png)

The pipeline is orchestrated using Apache Airflow with the following task dependencies:
- **start_pipeline** → **extract_data** → **transform_data** → **load_data** → **end_pipeline**

### Data Flow
1. **Source Data**: CSV file containing food delivery transactions
2. **Extract**: Load raw data into PostgreSQL `raw_data` schema
3. **Transform**: Clean, validate, and prepare data with comprehensive error handling
4. **Load**: Populate star schema dimensions and fact table
5. **Orchestration**: Airflow DAG manages the entire workflow with dependencies

### Database Schemas
- **`raw_data`**: Staging area for raw CSV data
- **`star_schema`**: Production data warehouse with dimensional model

### Docker Compose Services
- **PostgreSQL**: Primary database (Port 5432)
- **PgAdmin**: Database administration interface (Port 5050)
- **Airflow Webserver**: Pipeline monitoring and management (Port 8080)
- **Airflow Scheduler**: Background task scheduling and execution

## 🔄 Pipeline Features

### Data Quality & Validation
- **Missing Value Handling**: Comprehensive null value detection and handling
- **Data Type Conversion**: Automatic type casting with error handling
- **Duplicate Detection**: Remove duplicate records while preserving data integrity
- **Critical Field Validation**: Ensures essential fields are present before processing

### Error Handling & Monitoring
- **Comprehensive Logging**: Detailed logging at every pipeline stage
- **Transaction Management**: Database transactions with rollback capabilities
- **Retry Logic**: Airflow retry mechanisms for failed tasks
- **Email Notifications**: Configurable alerts for pipeline failures

### Performance Optimization
- **Table Truncation**: Prevents data duplication with CASCADE operations
- **Batch Processing**: Efficient bulk data operations
- **Index Management**: Optimized database performance
- **Connection Pooling**: Efficient database connection management
