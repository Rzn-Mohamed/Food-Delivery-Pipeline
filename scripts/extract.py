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
SCHEMA_NAME ="raw_data"

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
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"))
        logging.info(f"Schema'{SCHEMA_NAME}' checked/created")
        
def load_csv_to_db(engine):
    try:
        df = pd.read_csv(FILE_PATH)
        logging.info(f"File loaded to Db successfully")
        
        df.to_sql(name = TABLE_NAME , 
                  con=engine , 
                  schema = SCHEMA_NAME,
                  if_exists="replace",
                  index=False
                  )
        logging.info(f"Data loaded into {SCHEMA_NAME}.{TABLE_NAME} successfully")
    except Exception as e:
        logging.error("CSV load failed")
        raise e

def main():
    logging.info("EXTRACTING ...")
    engine = create_conn()
    db_schema(engine)
    load_csv_to_db(engine)
    logging.info("EXTRACTION FINISHED")
    
if __name__ == "__main__":
    main()