import os 
import pandas as pd 
import numpy as np
import logging
from sqlalchemy import create_engine , text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO , format="%(asctime)s - %(levelname)s - %(message)s")

SQL_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
TABLE_NAME= "deliveries_raw"
SCHEMA_NAME ="raw_data"

def create_conn():
    try:
        engine = create_engine(SQL_CONN)
        logging.info("Connected to DB")
        return engine
    except Exception as e :
        logging.error("Connection Failed")
        raise e

def extract_raw_from_db(engine):
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}"), conn)
            logging.info(f"Raw data extracted from {SCHEMA_NAME}.{TABLE_NAME} - {len(df)} records")
            return df
    except Exception as e:
        logging.error(f"Failed to extract raw data: {e}")
        raise e


def cleaning(df):
    logging.info("Starting data cleaning process...")
    
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], format="mixed", errors="coerce", dayfirst=True)
    df["Time_Orderd"] = pd.to_datetime(df["Time_Orderd"], format="%H:%M", errors="coerce").dt.time
    df["Time_Order_picked"] = pd.to_datetime(df["Time_Order_picked"], format="%H:%M", errors="coerce").dt.time
    
    df["Delivery_person_Age"] = pd.to_numeric(df["Delivery_person_Age"], errors='coerce').fillna(0).astype(int)
    df["Delivery_person_Ratings"] = pd.to_numeric(df["Delivery_person_Ratings"], errors='coerce').fillna(0).astype(float)
    df["Restaurant_latitude"] = pd.to_numeric(df["Restaurant_latitude"], errors='coerce')
    df["Restaurant_longitude"] = pd.to_numeric(df["Restaurant_longitude"], errors='coerce')
    df["Delivery_location_latitude"] = pd.to_numeric(df["Delivery_location_latitude"], errors='coerce')
    df["Delivery_location_longitude"] = pd.to_numeric(df["Delivery_location_longitude"], errors='coerce')
    df["Time_taken (min)"] = pd.to_numeric(df["Time_taken (min)"], errors='coerce')
    
    df["multiple_deliveries"] = pd.to_numeric(df["multiple_deliveries"], errors='coerce').fillna(0).astype(int)
    
    df["Weather_conditions"] = df["Weather_conditions"].str.strip().str.title()
    df["Road_traffic_density"] = df["Road_traffic_density"].str.strip().str.title()
    df["Vehicle_condition"] = pd.to_numeric(df["Vehicle_condition"], errors='coerce').fillna(0).astype(int)
    df["Type_of_order"] = df["Type_of_order"].str.strip().str.title()
    df["Type_of_vehicle"] = df["Type_of_vehicle"].str.strip().str.lower().str.replace(" ", "_")
    df["Festival"] = df["Festival"].str.strip().str.title()
    df["City"] = df["City"].str.strip().str.title()
    
    critical_fields = ["ID", "Delivery_person_ID", "Restaurant_latitude", "Restaurant_longitude", 
                      "Delivery_location_latitude", "Delivery_location_longitude", "Order_Date",
                      "Time_Orderd", "Time_Order_picked", "Type_of_order", "City", "Time_taken (min)"]

    initial_rows_count = len(df)
    df = df.replace(["", " ", "N/A", "n/a", "nan", "None", "none", None, np.nan], pd.NA)
    df = df.dropna(subset=critical_fields)
    dropped_rows_count = initial_rows_count - len(df)
    logging.info(f"{dropped_rows_count} rows with critical missing fields dropped!")
    
    initial_rows_count = len(df)
    df = df.drop_duplicates(keep='first')
    duplicate_count = initial_rows_count - len(df)
    logging.info(f"{duplicate_count} duplicate rows dropped!")
    
    # Coordinate validation
    initial_rows_count = len(df)
    df = df[
        (df['Restaurant_latitude'].between(-90, 90)) & 
        (df['Restaurant_longitude'].between(-180, 180)) &
        (df['Delivery_location_latitude'].between(-90, 90)) & 
        (df['Delivery_location_longitude'].between(-180, 180)) &
        (df['Restaurant_latitude'] != 0) & 
        (df['Restaurant_longitude'] != 0) &
        (df['Delivery_location_latitude'] != 0) & 
        (df['Delivery_location_longitude'] != 0)
    ]
    invalid_coords_count = initial_rows_count - len(df)
    logging.info(f"{invalid_coords_count} rows with invalid coordinates dropped!")
    
    logging.info(f"Data cleaning completed. Final record count: {len(df)}")
    return df
    


    
    