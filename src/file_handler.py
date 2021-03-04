# importing the relevant packages
import pandas as pd
import numpy as np


# importing the relevant modulels
from src.db.core import connection, db_update, db_query, db_search
from src.ETL.extract.core import drop_sensitive_info, extract_csv
from src.ETL.transform.core import (
    normalise_items,
    fill_null_values,
    load_unique_locations,
)

# setting up the connection
conn = connection()


# EXTRACT
# load data into the program
df = extract_csv("data/isle-of-wight.csv")

# drop personally identifiable information
drop_sensitive_info(df)


# TRANSFORM

normalise_items(df)
fill_null_values(df)


load_unique_locations(conn, db_update, db_search, df)

load_unique_products(conn, db_update, db_search, df)


# populate the purchase table and transaction table
for idx in range(len(df)):
    total_price = df["price"][idx]
    payment_type = df["payment-type"][idx]
    purchase_time = pd.Timestamp(df["datetime"][idx])
    location_variable = df["location"][idx]

    # we need to find the id in the database for the location
    search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
    values = {"location_name": location_variable}
    result = db_search(conn, search_location, values)
    location_id = result[0][0]

    # we can now populate the purchase table
    # first we need to check if they are unique
    search_purchase = "SELECT * FROM purchase WHERE total_price=%(total_price)s AND payment_type=%(payment_type)s AND purchase_time=%(purchase_time)s AND location_id=%(location_id)s"
    values = {
        "total_price": "%.2f" % total_price,
        "payment_type": payment_type,
        "purchase_time": purchase_time,
        "location_id": location_id,
    }
    result = db_search(conn, search_purchase, values)
    if result == []:
        sql = "INSERT INTO purchase (total_price, payment_type, purchase_time, location_id) VALUES (%(total_price)s,%(payment_type)s,%(purchase_time)s,%(location_id)s)"
        values = {
            "total_price": total_price,
            "payment_type": payment_type,
            "purchase_time": purchase_time,
            "location_id": location_id,
        }
        db_update(conn, sql, values)

    # let's populate the transaction table

    # first find the purchase id

    search_purchase = "SELECT purchase_id FROM purchase WHERE total_price=%(total_price)s AND payment_type=%(payment_type)s AND purchase_time=%(purchase_time)s AND location_id=%(location_id)s"
    values = {
        "total_price": "%.2f" % total_price,
        "payment_type": payment_type,
        "purchase_time": purchase_time,
        "location_id": location_id,
    }
    purchase_id = db_search(conn, search_purchase, values)[0][0]

    length = len(df["items"][idx])
    no_of_items = int(length / 3)
    for position in range(2, length, 3):
        # get the transaction price for the table
        transaction_price = df["items"][idx][position]
        product_name = df["items"][idx][position - 1]
        product_size = df["items"][idx][position - 2]

        # search for the matching product id
        search_for_product_id = "SELECT product_id FROM product WHERE product_name=%(product_name)s AND product_size=%(product_size)s"
        values = {"product_name": product_name, "product_size": product_size}
        product_variable = db_search(conn, search_for_product_id, values)
        if product_variable != []:
            product_id = product_variable[0][0]

            check = "SELECT * FROM transaction WHERE product_id=%(product_id)s AND purchase_id=%(purchase_id)s AND transaction_price=%(transaction_price)s"
            values = {
                "product_id": product_id,
                "purchase_id": purchase_id,
                "transaction_price": "%.2f" % float(transaction_price),
            }
            final_check = db_search(conn, check, values)
            if final_check == []:
                add_transaction = "INSERT INTO transaction (product_id, purchase_id, transaction_price) VALUES (%(product_id)s,%(purchase_id)s,%(transaction_price)s)"
                values = {
                    "product_id": product_id,
                    "purchase_id": purchase_id,
                    "transaction_price": transaction_price,
                }
                db_update(conn, add_transaction, values)
