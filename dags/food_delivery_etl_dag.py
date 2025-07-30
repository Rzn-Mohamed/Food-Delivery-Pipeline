from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.email import EmailOperator # type: ignore
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
import sys
import os
import logging

sys.path.append('/opt/airflow/scripts')

try:
    from ETL import Extract, Transform, Load # type: ignore 
    from extract import create_conn # type: ignore
except ImportError as e:
    logging.error(f"Failed to import ETL modules: {e}")
    Extract = Transform = Load = create_conn = None



default_args = {
    'owner': 'Razin',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'food_delivery_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for food delivery data - Portfolio Project',
    catchup=False,
    max_active_runs=1,
)

def extract_task(**context):
    try:
        if Extract is None or create_conn is None:
            raise ImportError("ETL modules not available")
            
        logging.info("Starting data extraction...")
        engine = create_conn()
        raw_df = Extract(engine)
        logging.info(f"Extraction completed: {len(raw_df)} records extracted")
        
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise e
    finally:
        if 'engine' in locals():
            engine.dispose()

def transform_task(**context):
    try:
        if Extract is None or Transform is None or create_conn is None:
            raise ImportError("ETL modules not available")
            
        logging.info("Starting data transformation...")
        engine = create_conn()
        raw_df = Extract(engine)
        cleaned_df = Transform(engine, raw_df)
        logging.info(f"Transformation completed: {len(cleaned_df)} records transformed")
        
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise e
    finally:
        if 'engine' in locals():
            engine.dispose()

def load_task(**context):
    try:
        if Extract is None or Transform is None or Load is None or create_conn is None:
            raise ImportError("ETL modules not available")
            
        logging.info("Starting data loading...")
        engine = create_conn()
        raw_df = Extract(engine)
        cleaned_df = Transform(engine, raw_df)
        Load(engine, cleaned_df)
        logging.info(f"Loading completed: {len(cleaned_df)} records loaded")
        
    except Exception as e:
        logging.error(f"Loading failed: {e}")
        raise e
    finally:
        if 'engine' in locals():
            engine.dispose()

start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag,
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)


start_pipeline >> extract >> transform >> load >> end_pipeline