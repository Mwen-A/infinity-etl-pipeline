import pandas as pd


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


# this may be used to speed up the loading process
# it sends a single query off to the database to get the id of the location
# this initializes the function for filling the purchases and transactions
def grab_first_location(df):
    return df["location"][0].title()