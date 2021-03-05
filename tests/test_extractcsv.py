
import pandas as pd
from io import StringIO
import pytest
from unittest.mock import Mock, MagicMock, patch
from pandas.testing import assert_frame_equal
from src.ETL.extract.core import extract_csv

mock_data = StringIO("""2020-12-20,ldn,Momo,Car,CASH,50,None""")
        
mock_df_raw = {
    'datetime': ['2020-12-20'],
    'location': ['ldn'],
    'name': ['Momo'],
    'items': ['Car'],
    'payment-type': ['CASH'],
    'price': [50],
    'payment-details': ['None'],
}

mock_headers = pd.DataFrame(
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

def test_addheaders_ecsv():
    actual = extract_csv(mock_data)
    expected = mock_headers

    assert_frame_equal(actual, expected)
    assert (actual == expected).all()[0]
    
def test_filepatherror_ecsv():
    with pytest.raises(IOError):
        extract_csv("notavalidpath")
    
test_addheaders_ecsv()
test_filepatherror_ecsv()