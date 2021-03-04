# importing the relevant packages
import pandas as pd
import numpy as np


# importing the relevant modulels
from src.db.core import connection, db_update, db_query, db_search


# setting up the connection
conn = connection()


# EXTRACT
def extract_csv(file_path: str):
    headers = [
        "datetime",
        "location",
        "name",
        "items",
        "payment-type",
        "price",
        "payment-details",
    ]
    df = pd.read_csv(file_path, names=headers)
    return df


df = extract_csv("data/isle-of-wight.csv")


# TRANSFORM
to_drop = ["name", "payment-details"]
df.drop(columns=to_drop, inplace=True)
df.drop_duplicates()
df.dropna()

for i in range(len(df["items"])):
    df["items"][i] = df["items"][i].split(",")

for basket in df["items"]:
    for position in range(len(basket)):
        if position % 3 == 0:
            # then we know it relates to the size of the item
            if basket[position] == "":
                # if it is a null, replace with NaN
                basket[position] = "regular"


# create a dataframe for the unique locations
location_df = df["location"].unique()

# # load the locations into the database
for location in location_df:
    search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
    values = {"location_name": location}
    result = db_search(conn, search_location, values)
    if result == []:
        sql = "INSERT INTO location (location_name) VALUES (%(location_name)s)"
        values = {"location_name": location}
        db_update(conn, sql, values)


# # create another dataframe for the unique products
products_list = []
for row in df["items"]:
    no_of_items = int(len(row) / 3)
    for position in range(0, len(row), 3):
        products_list.append((row[position], row[position + 1]))
products_set = set(products_list)

# load the unique products into the database
for size, name in products_set:
    search_product = "SELECT * FROM product WHERE product_name=%(product_name)s AND product_size=%(product_size)s"
    values = {"product_name": name, "product_size": size}
    result = db_search(conn, search_product, values)
    if result == []:
        sql = "INSERT INTO product (product_name, product_size) VALUES (%(product_name)s,%(product_size)s)"
        values = {"product_name": name, "product_size": size}
        db_update(conn, sql, values)

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
