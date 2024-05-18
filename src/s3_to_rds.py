import pandas as pd
import boto3
import os
import json
import psycopg2
from sqlalchemy import create_engine

config_path = os.path.join(os.path.dirname(__file__), '../config')
rds_config = os.path.join(config_path, 'rds_config.json')


with open(rds_config) as file:
    config = json.load(file)

conn = psycopg2.connect(**config, sslrootcert="SSLCERTIFICATE")

bucket = "air-traffic-data"

s3 = boto3.client("s3")
objects = s3.list_objects(Bucket=bucket)

files_list = []

for obj in objects['Contents']:
    if obj['Key'].endswith('.csv'):
        files_list.append(obj['Key'])

dataframes_list = []
for file in files_list:
    response = s3.get_object(Bucket=bucket, Key=file)
    temp_df = pd.read_csv(response.get("Body"))
    dataframes_list.append(temp_df)

df1, df2, df3 = dataframes_list


table1 = 'airlinewise_monthly_international'
table2 = 'citypairwise_quarterly_international'
table3 = 'countrywise_quarterly_international'

# Creating table in AWS RDS Postgresql
""" try:
    cur = conn.cursor()
    cur.execute(f'''
            CREATE TABLE IF NOT EXISTS {table1}(
                year TEXT,
                month TEXT,
                quarter TEXT,
                airline_name TEXT,
                carrier_type TEXT,
                passengers_to_india TEXT,
                passengers_from_india TEXT,
                freight_to_india TEXT,
                freight_from_india TEXT
                )''')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {table2}(
                year TEXT,
                quarter TEXT,
                city1 TEXT,
                city2 TEXT,
                passengers_from_city1_to_city2 TEXT,
                passengers_from_city2_to_city1 TEXT,
                freight_from_city1_to_city2 TEXT,
                freight_from_city2_to_city1 TEXT
                )''')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {table3}(
                year TEXT,
                quarter TEXT,
                country_name TEXT,
                passengers_to_india TEXT,
                passengers_from_india TEXT,
                freight_to_india TEXT,
                freight_from_india TEXT
                )''')
    conn.commit()
    # cur.execute("DROP TABLE countrywise_quarterly_international_air_traffic_to_and_from_the")
    # conn.commit()
    # print('Table dropped successfully!')
    # cur.execute(f"SELECT * from {table1}")
    # result = cur.fetchall()
    # print(result)
    conn.close()
except Exception as e:
    print(f'Database connection failed due to {e}') """

# Creating SQLAlchemy engine for df.to_sql to work
engine = create_engine(
    'postgresql://{user}:{password}@{host}:{port}/{database}'.format(**config))

# Moving data from csv file to tables in RDS
print(df1.to_sql(table1, engine, if_exists='replace', index=False),
      df2.to_sql(table2, engine, if_exists='replace', index=False),
      df3.to_sql(table3, engine, if_exists='replace', index=False))
