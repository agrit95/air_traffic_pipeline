import psycopg2
import boto3
import os
import sys
import pandas as pd

ENDPOINT="air-traffic-db.czykgioo6e9l.ca-central-1.rds.amazonaws.com"
PORT="5432"
DBNAME="postgres"


engine = psycopg2.connect(
    host='air-traffic-db.czykgioo6e9l.ca-central-1.rds.amazonaws.com',
    port='5432',
    user='postgres',
    password='airpostgres'
    # database='postgres'
)

curs = engine.cursor()

curs.execute('SELECT CURRENT_DATE;')

date = curs.fetchone()

print(date)
