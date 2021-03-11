import pandas as pd
from pandas.testing import assert_frame_equal
# step 1:
# define a test, eg: test_drop_sensitive_info():
# step 2:
# inject your dependencies eg: test_drop_sensitive_info(df):
# step 3: define your dataframe
# step 4: put the dataframe into the function
# step 5: define what you would expect
# step 6: compare what you expectation are to reality


# to remember: this function not only drops sensitive info
# if cleans up the database a little
def drop_sensitive_info(df):
    to_drop = ["name", "payment-details"]
    df.drop(columns=to_drop, inplace=True)
    df.drop_duplicates()
    df.dropna()
    return df

# test 1
def test_drop_sensitive_info_full_drop():
    
    mock_df_raw = {
        'datetime': ['2020-12-20'],
        'location': ['ldn'],
        'name': ['Momo'],
        'items': ['Car'],
        'payment-type': ['CASH'],
        'price': [50],
        'payment-details': ['None'],
    }
    
    initial_dataframe = pd.DataFrame(
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
    

    expected = pd.DataFrame(
        mock_df_raw,
        columns=[
            'datetime',
            'location',
            'items',
            'payment-type',
            'price',
        ]
    )
    
    
    actual_result = drop_sensitive_info(initial_dataframe)
    
    
    assert_frame_equal(actual_result, expected)

# test 2
def test_drop_sensitive_info_duplicate():
    mock_df_raw = {
        'datetime': ['2020-12-20','2020-12-20'],
        'location': ['ldn','ldn'],
        'name': ['Momo','Momo'],
        'items': ['Car','Car'],
        'payment-type': ['CASH','CASH'],
        'price': [50,50],
        'payment-details': ['None','None'],
    }
    
    initial_dataframe = pd.DataFrame(
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
    
    expected = pd.DataFrame(
        mock_df_raw,
        columns=[
            'datetime',
            'location',
            'items',
            'payment-type',
            'price',
        ]
    )
    
    actual_result = drop_sensitive_info(initial_dataframe)
    assert_frame_equal(actual_result, expected)

# test 3
def test_drop_sensitive_info_na():
    mock_df_raw = {
        'datetime': ['2020-12-20','2020-12-20'],
        'location': ['ldn','NaN'],
        'name': ['Momo','Momo'],
        'items': ['Car','Car'],
        'payment-type': ['CASH','CASH'],
        'price': [50,50],
        'payment-details': ['None','None'],
    }
    
    initial_dataframe = pd.DataFrame(
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
    
    expected = pd.DataFrame(
        mock_df_raw,
        columns=[
            'datetime',
            'location',
            'items',
            'payment-type',
            'price',
        ]
    )
    
    actual_result = drop_sensitive_info(initial_dataframe)
    assert_frame_equal(actual_result, expected)

# test 4
def test_drop_sensitive_info_column_mismatch():
    mock_df_raw = {
        'datetime': ['2020-12-20','2020-12-20'],
        'loctaion': ['ldn','ldn'], # this column is spelled wrong, let's see what the function does
        'name': ['Momo','Momo'],
        'items': ['Car','Car'],
        'payment-type': ['CASH','CASH'],
        'price': [50,50],
        'payment-details': ['None','None'],
    }
    
    initial_dataframe = pd.DataFrame(
        mock_df_raw,
        columns=[
            'datetime',
            'loctaion', # carry the spelling mistake over
            'name',
            'items',
            'payment-type',
            'price',
            'payment-details',
        ]
    )
    
    expected = pd.DataFrame(
        mock_df_raw,
        columns=[
            'datetime',
            'loctaion',
            'items',
            'payment-type',
            'price',
        ]
    )
    
    actual_result = drop_sensitive_info(initial_dataframe)
    assert_frame_equal(actual_result, expected)