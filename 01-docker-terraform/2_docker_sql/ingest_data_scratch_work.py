import argparse

import os
import pandas as pd 
from sqlalchemy import create_engine
from time import time


def main(params):
    """
    """
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # Initialize output.csv
    csv_name = 'output.csv'

    # Download the CSV
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name)


    # df.head(n = 0)
    
    df.to_sql(name = table_name, con = engine, if_exists = 'append')





if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help = 'username for Postgres')
    parser.add_argument('--password', help = 'password for Postgres')
    parser.add_argument('--host', help = 'host for Postgres')
    parser.add_argument('--port', help = 'port for Postgres')
    parser.add_argument('--db', help = 'database name for Postgres')
    parser.add_argument('--table_name', help = 'table name where we will write the results to')
    parser.add_argument('--url', help = 'URL of the CSV file')

    # Parse arguments
    args = parser.parse_args()

    # Run function
    main(args)


