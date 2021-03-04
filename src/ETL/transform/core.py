import pandas as pd
from src.db.core import connection, db_update,db_search


def normalise_items(df):
    for i in range(len(df["items"])):
        df["items"][i] = df["items"][i].split(",")
    return df

def fill_null_values(df):
    for basket in df["items"]:
        for position in range(len(basket)):
            if position % 3 == 0:
                # then we know it relates to the size of the item
                if basket[position] == "":
                    # if it is a null, replace with NaN
                    basket[position] = "Regular"
    return df

def load_unique_locations(conn,db_update,db_search, df):
    # create a dataframe for the unique locations
    location_df = df["location"].unique()

    # # load the locations into the database
    for location in location_df:
        search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
        values = {"location_name": location.title()}
        result = db_search(conn, search_location, values)
        if result == []:
            sql = "INSERT INTO location (location_name) VALUES (%(location_name)s)"
            values = {"location_name": location.title()}
            db_update(conn, sql, values)
    return df

def load_unique_products(conn, db_update,db_search,df):
    # create another dataframe for the unique products
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