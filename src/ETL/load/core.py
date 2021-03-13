import pandas as pd
from src.db.core import connection, db_update, db_search, db_update_many
import uuid

def load_unique_locations(conn, db_update, db_search, df):
    # create a dataframe for the unique locations
    location_df = df["location"].unique()

    # # load the locations into the database
    sql = "INSERT INTO location (location_id, location_name) VALUES %s"
    args_list = []
    for location in location_df:
        search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
        values = {"location_name": location.title()}
        result = db_search(conn, search_location, values)
        if result == []:
            location_id = str(uuid.uuid4())
            values = (location_id, location.title())
            args_list.append(values)
    db_update_many(conn, sql, args_list)


def load_unique_products(conn, db_update, db_search, products_set):
    # load the unique products into the database
    args_list = []
    sql = "INSERT INTO product (product_id, product_name, product_size) VALUES %s"
    for prod_id, size, name in products_set:
        search_product = "SELECT * FROM product WHERE product_size=%(product_size)s AND product_name=%(product_name)s"
        values = {"product_id": prod_id, "product_name": name, "product_size": size}
        result = db_search(conn, search_product, values)
        if result == []:
            values = (prod_id, name, size)
            args_list.append(values)
    db_update_many(conn, sql, args_list)


def load_purchase_transaction(conn, db_update, db_search, df, loc):
    search_location = (
        "SELECT location_id FROM location WHERE location_name=%(location_name)s"
    )
    values = {"location_name": loc}
    loc_result = db_search(conn, search_location, values)
    loc_result = [list(loc_result[0])]
    for idx in range(len(df)):
        total_price = df["price"][idx]
        payment_type = df["payment-type"][idx]
        purchase_time = pd.Timestamp(df["datetime"][idx])
        location_variable = df["location"][idx].title()

        # we need to find the id in the database for the location, we can save checks by using the check that is outside the for loop
        if location_variable != loc:
            search_location = (
                "SELECT location_id FROM location WHERE location_name=%(location_name)s"
            )
            values = {"location_name": location_variable}
            result = db_search(conn, search_location, values)
            location_id = result[0][0]
            loc = location_variable
            loc_result[0][0] = location_id
        else:
            location_id = loc_result[0][0]

        # we can now populate the purchase table
        purchase_id = str(uuid.uuid4())
        sql = "INSERT INTO purchase (purchase_id, total_price, payment_type, purchase_time, location_id) VALUES (%(purchase_id)s,%(total_price)s,%(payment_type)s,%(purchase_time)s,%(location_id)s)"
        values = {
            "purchase_id": purchase_id,
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
        for position in range(2, length, 3):
            # get the transaction price for the table
            transaction_price = df["items"][idx][position]
            product_name = df["items"][idx][position - 1].title()
            product_size = df["items"][idx][position - 2].title()

            # search for the matching product id
            search_for_product_id = "SELECT product_id FROM product WHERE product_name=%(product_name)s AND product_size=%(product_size)s"
            values = {"product_name": product_name, "product_size": product_size}
            product_variable = db_search(conn, search_for_product_id, values)

            product_id = product_variable[0][0]
            transaction_id = str(uuid.uuid4())
            add_transaction = "INSERT INTO transaction (transaction_id, product_id, purchase_id, transaction_price) VALUES (%(transaction_id)s,%(product_id)s,%(purchase_id)s,%(transaction_price)s)"
            values = {
                "transaction_id": transaction_id,
                "product_id": product_id,
                "purchase_id": purchase_id,
                "transaction_price": transaction_price,
            }
            db_update(conn, add_transaction, values)
