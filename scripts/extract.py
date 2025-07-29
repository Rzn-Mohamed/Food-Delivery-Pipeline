import os
import pandas as pd 
import logging
from sqlalchemy import create_engine , text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO , format="%(asctime)s - %(levelname)s - %(message)s")

#sqlalchemy conn
SQL_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

FILE_PATH = "data/source/Deliveries.csv"
TABLE_NAME= "deliveries_raw"
RAW_SCHEMA ="raw_data"

def create_conn():
    try:
        engine = create_engine(SQL_CONN)
        logging.info("Connected to Database")
        return engine
    except Exception as e:
        logging.error("Failed Connectiion")
        raise e

def db_schema(engine):
    with engine.connect() as conn :
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"))
        logging.info(f"Schema'{RAW_SCHEMA}' checked/created")

def extract_raw_from_db(engine):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {RAW_SCHEMA}.{TABLE_NAME}"), conn)
            logging.info(f"Raw data extracted from {RAW_SCHEMA}.{TABLE_NAME} - {len(df)} records")
            return df
    except Exception as e:
        logging.error(f"Failed to extract raw data: {e}")
        raise e

def load_csv_to_db(engine):
    try:
        df = pd.read_csv(FILE_PATH)
        logging.info(f"File loaded to Db successfully")
        
        df.to_sql(name = TABLE_NAME , 
                  con=engine , 
                  schema = RAW_SCHEMA,
                  if_exists="replace",
                  index=False
                  )
        logging.info(f"Data loaded into {RAW_SCHEMA}.{TABLE_NAME} successfully")
    except Exception as e:
        logging.error("CSV load failed")
        raise e