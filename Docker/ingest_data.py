import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2
    url1 = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
    url2 = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    
    if url1.endswith('.csv.gz'):
        csv1= 'trips.csv.gz'
    else:
        csv1 = 'trips.csv'
    if url2.endswith('.csv.gz'):
        csv2= 'zones.csv.gz'
    else:
        csv2 = 'zones.csv'     

    os.system(f"wget {url1} -O {csv1}")
    os.system(f"wget {url2} -O {csv2}")   
    # create db connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv1,iterator = True,chunksize=100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name=table_name_1,con=engine,if_exists='replace')

    df.head(n=0).to_sql(name= table_name_1,con=engine,if_exists="append")
    c = 0
    while True:
        t_start = time()
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        try:
            df.to_sql(name= table_name_1,con=engine,if_exists="append")

            t_end = time()

            print('insrted another chunk..., took %.3f second' %(t_end-t_start))
            c += t_end-t_start
        except:
            break

    print(f"it took {c/60} minutes")

# zones
    print('inserting zones to database')
    df =pd.read_csv(csv2)
    df.to_sql(name=table_name_2, con=engine, if_exists='append')
    print('finished inserting zones to database')


if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest csv to postgres')
    # user , password,port,host,databse_name,table,url
    parser.add_argument('--user',help='username for postgress')
    parser.add_argument('--password',help='password for postgress')
    parser.add_argument('--host',help='host for postgress')
    parser.add_argument('--port',help='port for postgress')
    parser.add_argument('--db',help='database name for postgress')
    parser.add_argument('--table_name_1',help='table to write in ')
    parser.add_argument('--table_name_2',help='table to write in ')
    parser.add_argument('--url',help='url of csv file')

    args = parser.parse_args()
    main(args)
