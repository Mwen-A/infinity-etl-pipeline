from unittest.mock import Mock, patch
import pandas as pd
import uuid

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
        item = (product_id,) + item
        products_list_2.append(item)
    return products_list_2

def test_get_unique_products_basic():
    with patch("uuid.uuid4", return_value="testuuid"):
        mock_df_raw = {
            'datetime': ["2021-02-23 09:00:48"],
            'location': ["Isle of Wight"],
            'name': ["Morgan Berka"],
            'items': [["Large","Hot chocolate","2.9"]],
            'payment-type': ['CASH'],
            'price': [8.40],
            'payment-details': ['None'],
        }
        
        df = pd.DataFrame(
            mock_df_raw,
            columns=[
                'datetime',
                'location',
                'name',
                'items',
                'payment-type',
                'price',
                'payment-details',
            ]
        )
        
        expected = [("testuuid","Large","Hot Chocolate")]
        
        result = get_unique_products(df)
        
        assert expected == result