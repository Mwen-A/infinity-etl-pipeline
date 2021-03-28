import pytest
import pandas as pd
import uuid
import time
from unittest.mock import Mock, patch

mock_raw = {
    "datetime": ["2020-12-20"],
    "location": ["ldn"],
    "items": [["Regular", "item1"]],
    "payment-type": ["CASH"],
    "price": [50],
}

mock_df = pd.DataFrame(
    mock_raw, columns=["datetime", "location", "items", "payment-type", "price"]
)


def load_purchase_transaction(conn, db_update_many, db_search, df, loc):
    search_location = (
        "SELECT location_id FROM location WHERE location_name=%(location_name)s"
    )
    values = {"location_name": loc}
    loc_result = db_search(conn, search_location, values)
    loc_result = [list(loc_result[0])]
    purchase_args_list = []
    purchase_input_sql = "INSERT INTO purchase (purchase_id, total_price, payment_type, purchase_time, location_id) VALUES %s"
    transaction_args_list = []
    transaction_input_sql = "INSERT INTO transaction (transaction_id, product_id, purchase_id, transaction_price) VALUES %s"
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
        purchase_args_list.append(
            (purchase_id, total_price, payment_type, purchase_time, location_id)
        )

        # let's populate the transaction table

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
            transaction_args_list.append(
                (transaction_id, product_id, purchase_id, transaction_price)
            )

    db_update_many(conn, purchase_input_sql, purchase_args_list)
    db_update_many(conn, transaction_input_sql, transaction_args_list)


def test_db_calls():
    with patch("uuid.uuid4", return_value="mockuuid"):

        mock_conn = Mock()

        mock_db_update = Mock()
        mock_db_update.return_value = True

        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]

        loc = "ldn"

        load_purchase_transaction(
            mock_conn, mock_db_update, mock_db_search, mock_df, loc
        )

        assert mock_db_update.call_count == 2
        # assert mock_db_search.call_count == 2

def test_db_update_args():
    with patch("uuid.uuid4", return_value="mockuuid"):

        mock_conn = Mock()

        mock_db_update = Mock()
        mock_db_update.return_value = True

        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]

        loc = "ldn"

        load_purchase_transaction(
            mock_conn, mock_db_update, mock_db_search, mock_df, loc
        )

        purchase, transaction = mock_db_update.call_args_list

        assert type(purchase[0][1]) == str
        assert type(purchase[0][2]) == list
        assert type(transaction[0][1]) == str
        assert type(transaction[0][2]) == list

def test_db_search_args():
    with patch("uuid.uuid4", return_value="mockuuid"):

        mock_conn = Mock()

        mock_db_update = Mock()
        mock_db_update.return_value = True

        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]

        loc = "ldn"

        load_purchase_transaction(
            mock_conn, mock_db_update, mock_db_search, mock_df, loc
        )

        args1, args2 = mock_db_search.call_args_list

        assert isinstance(args1[0][1], str)
        assert isinstance(args1[0][2], dict)
        assert isinstance(args2[0][1], str)
        assert isinstance(args2[0][2], dict)

def test_db_update_values():
    with patch("uuid.uuid4", return_value="mockuuid"):

        mock_conn = Mock()

        mock_db_update = Mock()
        mock_db_update.return_value = True

        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]

        loc = "ldn"

        load_purchase_transaction(
            mock_conn, mock_db_update, mock_db_search, mock_df, loc
        )

        purchase, transaction = mock_db_update.call_args_list
        assert purchase[0][2][0][0] == "mockuuid"
        assert purchase[0][2][0][1] == 50
        assert purchase[0][2][0][2] == "CASH"
        assert str(purchase[0][2][0][3]) == '2020-12-20 00:00:00'
        assert purchase[0][2][0][4] == 1
        assert transaction[0][1][:6] == "INSERT"

def test_db_search_values():
    with patch("uuid.uuid4", return_value="mockuuid"):

        mock_conn = Mock()

        mock_db_update = Mock()
        mock_db_update.return_value = True

        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]

        loc = "Ldn"

        load_purchase_transaction(
            mock_conn, mock_db_update, mock_db_search, mock_df, loc
        )

        select_location = mock_db_search.call_args_list
        print(mock_db_search.call_args_list)
        assert select_location[0][0][1][:6] == "SELECT"
        assert select_location[0][0][2]["location_name"] == "Ldn"

def test_db_indexerror():
    with patch("uuid.uuid4", return_value="mockuuid"):
        with pytest.raises(IndexError):

            mock_conn = Mock()

            mock_db_update = Mock()
            mock_db_update.return_value = True

            mock_db_search = Mock()
            mock_db_search.return_value = []

            loc = "str"

            load_purchase_transaction(
                mock_conn, mock_db_update, mock_db_search, mock_df, loc
            )
