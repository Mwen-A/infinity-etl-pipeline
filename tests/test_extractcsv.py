import pytest
import pandas as pd
from io import StringIO

from unittest.mock import Mock, MagicMock, patch
from pandas.testing import assert_frame_equal, assert_series_equal

def extract_csv(file_path: str):
    headers = [
        "datetime",
        "location",
        "name",
        "items",
        "payment-type",
        "price",
        "payment-details",
    ]
    df = pd.read_csv(file_path, names=headers)
    return df

mock_df_raw = {
    "datetime": ["2020-12-20"],
    "location": ["ldn"],
    "name": ["Momo"],
    "items": ["Car"],
    "payment-type": ["CASH"],
    "price": [50],
    "payment-details": ["None"],
}

mock_headers = pd.DataFrame(
    mock_df_raw,
    columns=[
        "datetime",
        "location",
        "name",
        "items",
        "payment-type",
        "price",
        "payment-details",
    ],
)

def test_addheaders_ecsv():
    
    mock_data = StringIO("""2020-12-20,ldn,Momo,Car,CASH,50,None""")
    actual = extract_csv(mock_data)
    expected = mock_headers

    assert_series_equal(actual.iloc[0], expected.iloc[0])
    assert_frame_equal(actual, expected)
    assert (actual.shape == expected.shape)

def test_filepatherror_ecsv():
    with pytest.raises(IOError):
        extract_csv("notavalidpath")

test_addheaders_ecsv()
test_filepatherror_ecsv()