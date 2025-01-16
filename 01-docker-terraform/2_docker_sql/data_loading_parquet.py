import argparse, os, sys 
from time import time 
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tb = params.tb
    url = params.url

    # Get name of file from url 
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading file {file_name}...')

    # Download file from url
    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    # Create SQL Engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read file based on csv or parquet
    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows = 10)
        df_iter = pd.read_csv(file_name, chunksize = 100000)

    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size = 10)).to_pandas()
        df_iter = file.iter_batches(batch_size = 100000)
    else:
        print('Error. Only .csv or .parquet files allowed.')
        sys.exit()

    # Create the table 
    df.head(0).to_sql(name = tb, con = engine, if_exists = 'replace')

    # Insert values 
    t_start = time()
    count = 0

    for batch in df_iter:

        count += 1

        if '.parquet' in file_name: 
            batch_df = batch.to_pandas()
        else:
            batch_df = batch
        
        print(f'Inserting batch {count}...')

        b_start = time()
        batch_df.to_sql(name = tb, con = engine, if_exists = 'append')
        b_end = time()

        print('Batch inserted in {:.2f} seconds.'.format(b_end - b_start))

    t_end = time()
    print(f'All batches inserted in {t_end - t_start:.2f} seconds.')

if __name__ == '__main__':
    #Parsing arguments 
    parser = argparse.ArgumentParser(description='Loading data from .paraquet file link to a Postgres datebase.')

    parser.add_argument('--user', help='Username for Postgres.')
    parser.add_argument('--password', help='Password to the username for Postgres.')
    parser.add_argument('--host', help='Hostname for Postgres.')
    parser.add_argument('--port', help='Port for Postgres connection.')
    parser.add_argument('--db', help='Databse name for Postgres')
    parser.add_argument('--tb', help='Destination table name for Postgres.')
    parser.add_argument('--url', help='URL for .paraquet file.')

    args = parser.parse_args()
    main(args)