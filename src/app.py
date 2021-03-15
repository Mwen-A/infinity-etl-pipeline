# importing the relevant packages
import pandas as pd
import json
import boto3
import csv


# importing the relevant modulels
from src.db.core import (
    connection,
    db_update,
    db_query,
    db_search,
    db_create,
    create_location,
    create_product,
    create_purchase,
    create_transaction,
)

from src.ETL.extract.core import extract
from src.ETL.transform.core import transform
from src.ETL.load.core import load

# setting up the connection
# conn = connection()

# creating the tables
# db_create(conn, create_product)
# db_create(conn, create_location)
# db_create(conn, create_purchase)
# db_create(conn, create_transaction)

# EXTRACT
# load data into the program

# df = extract_csv("data/2021-02-23-isle-of-wight.csv")

# TRANSFORM
# drop personally identifiable information and clean up
# drop_sensitive_info(df)
# normalise_items(df)
# fill_null_values(df)


# before loading, get the first location for setup
# loc = grab_first_location(df)

# # LOAD
# load_unique_locations(conn, db_update, db_search, df)
# products_set = get_unique_products(df)
# load_unique_products(conn, db_update, db_search, products_set)
# load_purchase_transaction(conn, db_update, db_search, df, loc)

# on a clean creation:
# did 2 queries for the load_unique_locations
# did 108 queries for the load_unique_products
# did 4185 queries for the load_purchase_transaction
# leading to a total of: 4295 queries

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    s3_resource = boto3.resource("s3")
    s3_object = s3_resource.Object(bucket, key)
    raw = s3_object.get()["Body"].read().decode("utf-8").splitlines()
    
    raw = extract(bucket)
    df, loc, uniques = transform(raw)
    result = load(df, loc, uniques) # ---> send result to redshift?? 
    
    # print out first row of df returned from transform
    print(df[0])

    