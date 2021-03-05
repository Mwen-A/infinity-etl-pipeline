# importing the relevant packages
import pandas as pd
import numpy as np
import time

# importing the relevant modulels
from src.db.core import connection, db_update, db_query, db_search
from src.ETL.extract.core import drop_sensitive_info, extract_csv
from src.ETL.transform.core import (
    normalise_items,
    fill_null_values,
    grab_first_location,
)
from src.ETL.load.core import (
    load_unique_locations,
    load_unique_products,
    load_purchase_transaction,
)

# setting up the connection
conn = connection()


# EXTRACT
# load data into the program
start = time.time()
df = extract_csv("data/isle-of-wight.csv")

# TRANSFORM
# drop personally identifiable information and clean up
drop_sensitive_info(df)
normalise_items(df)
fill_null_values(df)


# before loading, get the first location for setup
loc = grab_first_location(df)

# # LOAD
load_unique_locations(conn, db_update, db_search, df)
load_unique_products(conn, db_update, db_search, df)
load_purchase_transaction(conn, db_update, db_search, df, loc)

# on a clean creation:
# did 2 queries for the load_unique_locations
# did 108 queries for the load_unique_products
# did 4185 queries for the load_purchase_transaction
# leading to a total of: 4295 queries
end = time.time()
print(end - start)
