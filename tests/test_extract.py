import pandas as pd
from src.ETL.transform.core import fill_null_values
from unittest.mock import Mock, patch




def test_fill_null_values():
    # ASSEMBLE
    data_frame = pd.DataFrame(
        {
            "items": [["", 4, 5, "", 6, 7, 8, "", 9], [4, "", 2], [3, 4, 6]],
            "name": [7, "", 9],
            "location": ["", 11, 12],
        }
    )

    # ACT
    fill_null_values(data_frame)

    # ASSERT
    assert data_frame["items"][0][0] == "Regular"
    assert data_frame["items"][0][3] == "Regular"
    assert data_frame["items"][0][7] == ""
    


