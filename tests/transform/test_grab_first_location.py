import pandas as pd
import pytest

def grab_first_location(df):
    return df["location"][0].title()

# takes a dataframe
# looks in the location column
# grabs the location that appears first



def test_grab_first_location():
    
    raw_data = {
        "location": ["Manchester","Liverpool"]
    }
    df = pd.DataFrame(raw_data,columns=["location"])

    result = grab_first_location(df)
    
    expected = "Manchester"
    
    assert result == expected

def test_grab_first_location_empty():
    with pytest.raises(KeyError):
        df = pd.DataFrame({"location": []})

        result = grab_first_location(df)
        print(result)