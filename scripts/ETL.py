import os
import logging
from dotenv import load_dotenv
from sqlalchemy import text
from extract import create_conn, db_schema, load_csv_to_db, extract_raw_from_db
from transform import cleaning, create_star_schema, create_star_schema_tables
from load import (
    populate_dim_delivery_person,
    populate_dim_location, 
    populate_dim_datetime, 
    populate_dim_vehicle, 
    populate_fact_deliveries)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clear_all_tables(engine):
    logging.info(" CLEARING ALL TABLES TO PREVENT DUPLICATION ")
    
    tables_to_clear = [
        'star_schema.fact_deliveries',
        'star_schema.dim_delivery_person', 
        'star_schema.dim_location',
        'star_schema.dim_datetime',
        'star_schema.dim_vehicle'
    ]
    
    try:
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                for table in tables_to_clear:
                    conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"))
                    logging.info(f"✅ Cleared table: {table}")
                
                transaction.commit()
                logging.info(" ALL TABLES CLEARED SUCCESSFULLY ")
                
            except Exception as e:
                transaction.rollback()
                logging.error(f"Error during table clearing, transaction rolled back: {e}")
                raise e
                
    except Exception as e:
        logging.error(f"FAILED TO CLEAR TABLES: {e}")
        raise e

def Extract(engine):
    logging.info(" STARTING EXTRACTION PHASE ")
    
    try:
        # Create schema
        db_schema(engine)
        
        # Load CSV to database
        load_csv_to_db(engine)
        
        # Extract raw data
        raw_df = extract_raw_from_db(engine)
        
        logging.info("EXTRACTION PHASE COMPLETED")
        return raw_df
        
    except Exception as e:
        logging.error(f"EXTRACTION PHASE FAILED: {e}")
        raise e

def Transform(engine, raw_df):
    logging.info(" STARTING TRANSFORMATION PHASE ")
    
    try:
        # Clean and transform data
        cleaned_df = cleaning(raw_df)
        
        # Create star schema
        create_star_schema(engine)
        
        # Create star schema tables
        create_star_schema_tables(engine)
        
        # Clear all existing data to prevent duplication
        clear_all_tables(engine)
        
        logging.info(" TRANSFORMATION PHASE COMPLETED ")
        logging.info(f"Original records: {len(raw_df)}")
        logging.info(f"Cleaned records: {len(cleaned_df)}")
        logging.info(f"Data reduction: {len(raw_df) - len(cleaned_df)} records removed")
        
        return cleaned_df
        
    except Exception as e:
        logging.error(f"TRANSFORMATION PHASE FAILED: {e}")
        raise e

def Load(engine, cleaned_df):
    
    logging.info(" STARTING LOADING PHASE ")
    
    try:
        # Populate dimension tables
        logging.info("Populating dimension tables...")
        populate_dim_delivery_person(cleaned_df, engine)
        populate_dim_location(cleaned_df, engine)
        populate_dim_datetime(cleaned_df, engine)
        populate_dim_vehicle(cleaned_df, engine)
        
        # Populate fact table
        logging.info("Populating fact table...")
        populate_fact_deliveries(cleaned_df, engine)
        
        logging.info(" LOADING PHASE COMPLETED ")
        logging.info(f"Total records processed: {len(cleaned_df)}")
        
    except Exception as e:
        logging.error(f"LOADING PHASE FAILED: {e}")
        raise e

def main():
    logging.info(" STARTING FOOD DELIVERY ETL PIPELINE ")
    
    try:
        # Create database connection
        engine = create_conn()
        
        # Run ETL phases
        raw_df = Extract(engine)
        cleaned_df = Transform(engine, raw_df)
        Load(engine, cleaned_df)
        
        logging.info(" ETL PIPELINE COMPLETED SUCCESSFULLY! ")
        logging.info(f"✅ Final Summary:")
        logging.info(f"   - Raw records processed: {len(raw_df)}")
        logging.info(f"   - Clean records loaded: {len(cleaned_df)}")
        logging.info(f"   - Data quality improvement: {((len(cleaned_df)/len(raw_df))*100):.2f}% retention rate")
        
    except Exception as e:
        logging.error(f"❌ ETL PIPELINE FAILED: {e}")
        raise e
    
    finally:
        # Close database connection if exists
        if 'engine' in locals():
            engine.dispose()
            logging.info("Database connections closed")

if __name__ == "__main__":
    main()
