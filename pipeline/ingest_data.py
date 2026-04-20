#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--url', required=True, type=str, help='Url to CSV file')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, url):
    chunksize = 100000

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if 'tpep_pickup_datetime' in df_chunk.columns:
            df_chunk.tpep_pickup_datetime = pd.to_datetime(df_chunk.tpep_pickup_datetime)
            df_chunk.tpep_dropoff_datetime = pd.to_datetime(df_chunk.tpep_dropoff_datetime)

        if first:
            df_chunk.to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace',
                index=False
                )
            first = False
        else:
            df_chunk.to_sql(
                name=target_table, 
                con=engine, 
                if_exists='append',
                index=False
                )

if __name__ == "__main__":
    run()


