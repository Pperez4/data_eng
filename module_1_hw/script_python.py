#-----------------------------
# Importing needed libraries 
#-----------------------------
import argparse, os, sys
from time import time
import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tb = params.tb
    url = params.url

    # get the name of the file from url
    file_name = url.rsplit('/',1)[-1].strip()
    print(f"Downloading {file_name} ...")
    # ----------------------------------------
    # download file
    # ----------------------------------------
    # url.strip() -> Removes any leading or trailing whitespace from url.
    # f'curl {url.strip()} -o {file_name}'
    # uses an f-string to insert the cleaned url into a curl command.
    # curl {url}: Downloads the content from the given URL.
    # -o{file_name}: Saves the downloaded content as file_name.
    os.system(f'curl {url.strip()} -o {file_name}')
    print('\n')

    # create sql engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read file based on csv or parquet

    if '.csv' in file_name:
        df = pd.read_csv(file_name, nrows=10)
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif '.parquet' in file_name:
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)
    else:
        print('Error. Only .csv or .parquet files allowed.')
        sys.exit()

    # create the table 
    df.head(0).to_sql(name=tb, con=engine, if_exists='replace')

    # insert values 
    t_start = time()
    count = 0
    for batch in df_iter:
        count+=1

        if '.parquet' in file_name:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f"Inserting batch {count}...")

 
        b_start = time()
        batch_df.to_sql(name=tb, con=engine, if_exists='append')
        b_end = time()

        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
    
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    

if __name__ == '__main__':
    #Parsing arguments 
    parser = argparse.ArgumentParser(description='Loading data from .paraquet file link to a Postgres datebase.')

    parser.add_argument('--user', help='Username for Postgres.')
    parser.add_argument('--password', help='Password to the username for Postgres.')
    parser.add_argument('--host', help='Hostname for Postgres.')
    parser.add_argument('--port', help='Port for Postgres connection.')
    parser.add_argument('--db', help='Database name for Postgres')
    parser.add_argument('--tb', help='Destination table name for Postgres.')
    parser.add_argument('--url', help='URL for .paraquet file.')

    args = parser.parse_args()
    main(args)