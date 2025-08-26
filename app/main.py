#!/usr/bin/env python3


import os
import sys
import httpx
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine

#Configure logging
logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s - [%(levelname)s] - %(message)s',
    stream = sys.stdout
)

#Get environment variables
base_dir = Path(__file__).resolve().parent.parent
load_dotenv(base_dir / '.env')

api_key = os.getenv('api_key')
api_url = os.getenv('api_url')
raw_file_path = base_dir / os.getenv('raw_data')
clean_file_path = base_dir / os.getenv('clean_data')
db_config = {
    'user': os.getenv('db_username'),
    'password': os.getenv('db_password'),
    'host': os.getenv('db_host'),
    'port': os.getenv('db_port'),
    'database': os.getenv('db_name'),
    'schema': os.getenv('db_schema'),
}
raw_table = os.getenv('bronze_table')
clean_table = os.getenv('silver_table')

#Extract data from url
def extract_data(url, key):
    headers = {
        'Accept': 'application/json',
        'X-Api-Key': key
    }

    try: 
        logging.info('Requesting data')
        response = httpx.get(url, headers=headers, timeout=10.0)
        response.raise_for_status()
        logging.info('Data extracted successfully')
        return response.json()
    except httpx.HTTPStatusError as e:
        logging.error(f'HTTP error {e.response.status_code}: {e.response.text}')
    except httpx.RequestError as e:
        logging.error(f'Network error while requesting {e.request.url!r}: {e}')
    except Exception as e:
        logging.exception(f'Unexpected error: {e}')
    return None

#load raw data to datastore
def load_raw_data(data, dir):
    if not data:
        logging.warning('No data to save')
        return False
    
    #Checking first record for nested structure
    first_record = data[0] if isinstance(data, list) else data

    if any(isinstance(v, (dict, list)) for v in first_record.values()):
        df = pd.json_normalize(data, sep='_')
        logging.info('data is a nested JSON')
    else:
        df = pd.DataFrame(data)
        logging.info('data is a flat JSON')

    df.to_csv(dir, index=False)
    logging.info(f'Raw data loaded to {dir}')
    return True

#load data to database
def load_to_database(dir, db, table):
    try:
        logging.info('Creating connection with database')
        engine = create_engine(
            f"postgresql+psycopg2://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        )

        #Test connection
        with engine.connect() as conn:
            logging.info('Database connection successful')
        
        df = pd.read_csv(dir)
        df.to_sql(table, engine, if_exists='replace', index=False, schema=db['schema'])
        logging.info(f"Data loaded into Database table: {table}")
        return True
    except Exception as e:
        logging.exception(f'Failed to load Data into Database table: {e}')
        return False


#Transform data
def transform_data(dir):
    df = pd.read_csv(dir)
    clean_df = df[[
        'id', 'formattedAddress', 'city','state', 'zipCode', 'county', 'latitude', 'longitude', 'propertyType',
       'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'yearBuilt',
       'status', 'price', 'listingType', 'listedDate', 'removedDate',
       'createdDate', 'lastSeenDate', 'daysOnMarket', 'mlsName', 'mlsNumber',
       'listingAgent_name', 'listingAgent_phone', 'listingAgent_email',
       'listingAgent_website', 'listingOffice_name', 'listingOffice_phone',
       'listingOffice_email', 'listingOffice_website'
    ]].copy()

    #Rename columns
    clean_df.rename(columns={
        'formattedAddress': 'full_address','zipCode': 'postal_code'
    }, inplace=True)

    #convert to snake_case
    clean_df.columns = (
        clean_df.columns
        .str.strip()
        .str.replace(r'([A-Z])', r'_\1', regex=True)
        .str.replace(" ", "_")
        .str.replace(r'[^0-9a-zA-Z_]', r'_', regex=True)
        .str.lower()
        .str.replace('__', '_')
        .str.strip('_')
    )

    logging.info('Data cleaned')
    return clean_df

#load clean data to datastore
def load_clean_data(df, dir):
    df.to_csv(dir, index=False)
    logging.info(f"Clean Data loaded to {dir}")
    return True


#Entry point to app
def main():
    raw_data = extract_data(api_url, api_key)
    if not raw_data:
        return 1
    
    if not load_raw_data(raw_data, raw_file_path):        
        return 1
    
    if not load_to_database(raw_file_path, db_config, raw_table):
        return 1
    
    clean_df = transform_data(raw_file_path)
    if clean_df is None or clean_df.empty:
        logging.error('No data after transformation')
        return 1
    
    if not load_clean_data(clean_df, clean_file_path):
        return 1
    
    if not load_to_database(clean_file_path, db_config, clean_table):
        return 1
    
    return 0
        

if __name__ == '__main__':
    sys.exit(main())


