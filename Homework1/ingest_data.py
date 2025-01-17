import pandas as pd
import os
from sqlalchemy import create_engine
import argparse
from time import time

GREEN_TAXI_CSV = r"green_tripdata_2019-10.csv.gz"
LOOKUP_CSV = r"taxi_zone_lookup.csv"

def main(params):
    # Read the gzipped CSV file directly

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    taxi_url = params.taxi_url
    lookup_url = params.lookup_url

    # download green taxi csv files
    os.system(f"curl -L -O {taxi_url}")
    os.system(f"curl -L -O {lookup_url}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    print("Load look up data")
    df_zones = pd.read_csv(LOOKUP_CSV)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')

    
    df_iter = pd.read_csv(GREEN_TAXI_CSV, iterator=True, chunksize=100000, compression="gzip")
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists='append')

    print("Loading Green taxi data")
    while True:
        try:
            # Process the next chunk
            df = next(df_iter)
            # Perform processing
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists="append")
        except StopIteration:
            # Exit the loop when done
            print("Reached the end of the df")
            break


"""
python command
python ingest_data.py --user "postgres" --password "password" --host "localhost" --port "5432" --db "ny_taxi" --taxi_url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz" --lookup_url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
docker command 
docker run --network=homework1_default taxi_ingest:w1 \
    --user "postgres" \
    --password "postgres" \
    --host "postgres" \
    --port "5432" \
    --db "ny_taxi" \
    --table_name "green_taxi_trips" \
    --taxi_url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz" \
    --lookup_url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument(
        "--table_name", help="name of the table where we will write the results to"
    )
    parser.add_argument("--taxi_url", help="url of the taxi csv file")
    parser.add_argument("--lookup_url", help="url of the lookup csv file")

    args = parser.parse_args()
    main(args)
