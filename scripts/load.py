import os 
import pandas as pd 
import numpy as np
import logging
from sqlalchemy import create_engine , text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO , format="%(asctime)s - %(levelname)s - %(message)s")

TABLE_NAME= "deliveries_raw"
FILE_PATH = "data/source/Deliveries.csv"
SQL_CONN = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
STAR_SCHEMA = "star_schema"
RAW_SCHEMA ="raw_data"


def populate_dim_delivery_person(df ,engine):
    try:
        #extract delivery people 
        delivery_people = df[["Delivery_person_ID" , "Delivery_person_Age" ,"Delivery_person_Ratings"]].copy()
        
        delivery_people = delivery_people.drop_duplicates(subset=['Delivery_person_ID'], keep='first')
        
        delivery_people = delivery_people.rename(
            columns={
                'Delivery_person_ID' : 'delivery_person_id',
                'Delivery_person_Age' : 'age',
                'Delivery_person_Ratings' : "ratings"
            })
        
        delivery_people.to_sql(
            name='dim_delivery_person',
            con=engine,
            schema = STAR_SCHEMA,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Inserted {len(delivery_people)} records into dim_delivery_person")
        
    except Exception as e:
        logging.error(f"FAILED TO INSERT RECORDS TO dim_delivery_person : {e}")
        raise e


def populate_dim_location(df ,engine):
    try:
        locations =[]
        
        
        for _ , row in df.iterrows():
            #restaurant locations
            locations.append({
                "latitude" : row['Restaurant_latitude'],
                "longitude":row['Restaurant_longitude'],
                "city":row['City'],
                "location_type" : "restaurant"
            })
            
            #delivery locations
            locations.append({
                'latitude': row['Delivery_location_latitude'],
                'longitude':row['Delivery_location_longitude'],
                'city': row['City'],
                'location_type': 'delivery'
            })
            
        locations = pd.DataFrame(locations).drop_duplicates()
            
        locations.to_sql(
                name ='dim_location',
                con=engine,
                schema = STAR_SCHEMA,
                if_exists=  'append',
                index =False
            )
            
        logging.info(f"Inserted {len(locations)} records into dim_location")
        
    
    except Exception as e:
        logging.error(f"Failed to insert records to dim_location : {e}")
        raise e

def populate_dim_datetime(df, engine):
    try:
        # Get unique combinations of date and times
        datetimes = df[['Order_Date', 'Time_Orderd', 'Time_Order_picked']].drop_duplicates()
        
        datetime_records = []
        for _, row in datetimes.iterrows():
            datetime_records.append({
                'order_date': row['Order_Date'].date(),
                'time_ordered': row['Time_Orderd'],
                'time_picked': row['Time_Order_picked'],
                'day': row['Order_Date'].day,
                'month': row['Order_Date'].month,
                'year': row['Order_Date'].year
            })
        
        datetime_df = pd.DataFrame(datetime_records)
        
        
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for _, row in datetime_df.iterrows():
                    insert_query = text("""
                        INSERT INTO star_schema.dim_datetime (order_date, time_ordered, time_picked, day, month, year)
                        VALUES (:order_date, :time_ordered, :time_picked, :day, :month, :year)
                        ON CONFLICT (order_date, time_ordered, time_picked) DO NOTHING
                    """)
                    conn.execute(insert_query, {
                        'order_date': row['order_date'],
                        'time_ordered': row['time_ordered'],
                        'time_picked': row['time_picked'],
                        'day': row['day'],
                        'month': row['month'],
                        'year': row['year']
                    })
                transaction.commit()
                logging.info(f"Processed {len(datetime_df)} unique datetime combinations for dim_datetime")
                
            except Exception as e:
                transaction.rollback()
                raise e
        
    except Exception as e:
        logging.error(f"Failed to populate dim_datetime: {e}")
        raise e


def populate_dim_vehicle(df, engine):
    try:
        
        vehicles = df[['Vehicle_condition', 'Type_of_vehicle']].drop_duplicates()
        
        vehicles = vehicles.rename(columns={
            'Vehicle_condition': 'vehicle_condition',
            'Type_of_vehicle': 'vehicle_type'
        })
        
        vehicles.to_sql(
            name='dim_vehicle',
            con=engine,
            schema=STAR_SCHEMA,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Inserted {len(vehicles)} vehicles into dim_vehicle")
        
    except Exception as e:
        logging.error(f"Failed to populate dim_vehicle: {e}")
        raise e

def populate_fact_deliveries(df ,engine):
    try:
        logging.info("Fact table")
        
        fact_records=[]
        
        with engine.connect() as conn :
            for _ , row in df.iterrows():
                #delivery person keys
                delivery_person_query = text(f""" 
                        SELECT delivery_person_key
                        FROM {STAR_SCHEMA}.dim_delivery_person
                        WHERE delivery_person_id = :person_id
                """)
                delivery_person_res =conn.execute(delivery_person_query,
                                  {'person_id': row['Delivery_person_ID']}).fetchone()
                delivery_person_key = delivery_person_res[0] if delivery_person_res else None
                
                #restaurant keys
                restaurant_location_query = text(f"""
                    SELECT location_key 
                    FROM {STAR_SCHEMA}.dim_location 
                    WHERE latitude = :lat AND longitude = :long 
                    AND city = :city AND location_type = 'restaurant'
                """)
                restaurant_res = conn.execute(restaurant_location_query, {
                    'lat': row['Restaurant_latitude'],
                    'long': row['Restaurant_longitude'],
                    'city': row['City']
                }).fetchone()
                restaurant_location_key = restaurant_res[0] if restaurant_res else None
                
                # delivery location keys
                delivery_location_query = text(f"""
                    SELECT location_key 
                    FROM {STAR_SCHEMA}.dim_location 
                    WHERE latitude = :lat AND longitude = :long 
                    AND city = :city AND location_type = 'delivery'
                """)
                delivery_res = conn.execute(delivery_location_query, {
                    'lat': row['Delivery_location_latitude'],
                    'long': row['Delivery_location_longitude'],
                    'city': row['City']
                }).fetchone()
                delivery_location_key = delivery_res[0] if delivery_res else None
                
                #vehicles key
                vehicle_query = text(f"""
                    SELECT vehicle_key 
                    FROM {STAR_SCHEMA}.dim_vehicle 
                    WHERE vehicle_condition = :condition AND vehicle_type = :type
                """)
                vehicle_res = conn.execute(vehicle_query, {
                    'condition': row['Vehicle_condition'],
                    'type': row['Type_of_vehicle']
                }).fetchone()
                vehicle_key = vehicle_res[0] if vehicle_res else None
                
                #datetime keys
                datetime_query = text(f"""
                    SELECT datetime_key 
                    FROM {STAR_SCHEMA}.dim_datetime 
                    WHERE order_date = :date AND time_ordered = :time_ordered AND time_picked = :time_picked
                """)
                datetime_res = conn.execute(datetime_query, {
                    'date': row['Order_Date'].date(),
                    'time_ordered': row['Time_Orderd'],
                    'time_picked': row['Time_Order_picked']
                }).fetchone()
                datetime_key = datetime_res[0] if datetime_res else None
                
                fact_record = {
                    'delivery_id': row['ID'],
                    'delivery_person_key': delivery_person_key,
                    'restaurant_location_key': restaurant_location_key,
                    'delivery_location_key': delivery_location_key,
                    'vehicle_key': vehicle_key,
                    'datetime_key': datetime_key,
                    'order_type': row['Type_of_order'],
                    'weather_condition': row['Weather_conditions'],
                    'road_traffic_density': row['Road_traffic_density'],
                    'festival': row['Festival'],
                    'multiple_deliveries': row['multiple_deliveries'],
                    'time_taken': row['Time_taken (min)']
                }
                
                fact_records.append(fact_record)
        
        fact_df = pd.DataFrame(fact_records)
        
        fact_df.to_sql(
            name='fact_deliveries',
            con=engine,
            schema=STAR_SCHEMA,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Inserted {len(fact_df)} records into fact_deliveries")

    except Exception as e:
        logging.error(f"Failed to insert records to fact_deliveries: {e}")
        raise e