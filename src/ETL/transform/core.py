import pandas as pd
import uuid

def normalise_items(df):
    for i in range(len(df["items"])):
        df["items"][i].split(",")
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

def get_unique_products(df):
    # create another dataframe for the unique products
    products_list = []
    for row in df["items"]:
        for position in range(0, len(row), 3):
            products_list.append((row[position].title(), row[position + 1].title()))
    products_set = set(products_list)
    products_list_2 = []
    for item in products_set:
        product_id = str(uuid.uuid4())
        item = (product_id, ) + item
        products_list_2.append(item)
    return products_list_2


# this may be used to speed up the loading process
# it sends a single query off to the database to get the id of the location
# this initializes the function for filling the purchases and transactions
def grab_first_location(df):
    return df["location"][0].title()