import psycopg2
import sys
import boto3
import os
import json

config_path = os.path.join(os.path.dirname(__file__),'../config')
rds_config = os.path.join(config_path,'rds_config.json')


with open(rds_config) as file:
  config = json.load(file)



try:
    conn = psycopg2.connect(**config, sslrootcert="SSLCERTIFICATE")
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))


