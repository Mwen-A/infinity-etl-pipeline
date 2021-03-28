
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from pandas.testing import assert_frame_equal, assert_series_equal

def normalise_items(df):
    df["items"] = [x.split(",") for x in df["items"]]
    return df

def test_normalise_items():     

    mock_item = {'items': ['car,bus,train,plane']}
    mock_df_item = pd.DataFrame(mock_item, columns=['items'])

    normalise_items(mock_df_item)
    assert mock_df_item['items'][0][0] == 'car'
    assert mock_df_item['items'][0][1] == 'bus'
    assert mock_df_item['items'][0][2] == 'train'
    assert mock_df_item['items'][0][3] == 'plane'

test_normalise_items()