#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def ingest_parquet(params):
    """
    Ingest data from a parquet file into PostgreSQL
    """
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    file_path = params.file_path
    
    # Create connection to PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Read parquet file
    print(f"Reading parquet file: {file_path}")
    df = pd.read_parquet(file_path)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {df.columns.tolist()}")
    
    # Convert datetime columns if they exist
    datetime_columns = df.select_dtypes(include=['datetime64']).columns
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col])
    
    # Insert data in chunks
    chunk_size = 100000
    total_chunks = (len(df) // chunk_size) + 1
    
    print(f"Inserting data into table '{table_name}'...")
    
    for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
        t_start = time()
        chunk_end = min(chunk_start + chunk_size, len(df))
        chunk = df.iloc[chunk_start:chunk_end]
        
        if i == 0:
            # First chunk: create table
            chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        else:
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        t_end = time()
        print(f'Inserted chunk {i+1}/{total_chunks} ({chunk_end}/{len(df)} rows), took %.3f seconds' % (t_end - t_start))
    
    print("Data ingestion completed successfully!")


def ingest_csv(params):
    """
    Ingest data from a CSV file into PostgreSQL
    """
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    file_path = params.file_path
    
    # Create connection to PostgreSQL
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Read CSV file in chunks for large files
    print(f"Reading CSV file: {file_path}")
    
    # Read first to check structure
    df_sample = pd.read_csv(file_path, nrows=100)
    print(f"Columns: {df_sample.columns.tolist()}")
    
    # Detect datetime columns
    datetime_columns = []
    for col in df_sample.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            datetime_columns.append(col)
    
    print(f"Detected datetime columns: {datetime_columns}")
    
    # Read and insert in chunks
    chunk_size = 100000
    chunk_iterator = pd.read_csv(file_path, iterator=True, chunksize=chunk_size)
    
    print(f"Inserting data into table '{table_name}'...")
    
    for i, chunk in enumerate(chunk_iterator):
        t_start = time()
        
        # Convert datetime columns
        for col in datetime_columns:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col])
        
        if i == 0:
            # First chunk: create table
            chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        else:
            chunk.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        t_end = time()
        print(f'Inserted chunk {i+1} ({len(chunk)} rows), took %.3f seconds' % (t_end - t_start))
    
    print("Data ingestion completed successfully!")


def main(params):
    """
    Main function to determine file type and call appropriate ingest function
    """
    file_path = params.file_path
    
    # Determine file type from file path
    if file_path.endswith('.parquet'):
        print("Detected parquet file")
        ingest_parquet(params)
    elif file_path.endswith('.csv') or file_path.endswith('.csv.gz'):
        print("Detected CSV file")
        ingest_csv(params)
    else:
        print("Error: Unsupported file format. Please provide a .parquet or .csv file")
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV or Parquet data to PostgreSQL')
    
    parser.add_argument('--user', default='postgres', help='PostgreSQL user name (default: postgres)')
    parser.add_argument('--password', default='postgres', help='PostgreSQL password (default: postgres)')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', default='5433', help='PostgreSQL port (default: 5433)')
    parser.add_argument('--db', default='ny_taxi', help='PostgreSQL database name (default: ny_taxi)')
    parser.add_argument('--table_name', required=True, help='Table name to write to')
    parser.add_argument('--file_path', required=True, help='Local path to the CSV or parquet file')
    
    args = parser.parse_args()
    
    main(args)
