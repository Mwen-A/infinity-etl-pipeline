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



def drop_sensitive_info(df):
    to_drop = ["name", "payment-details"]
    df.drop(columns=to_drop, inplace=True)
    df.drop_duplicates()
    df.dropna()
    return df

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




