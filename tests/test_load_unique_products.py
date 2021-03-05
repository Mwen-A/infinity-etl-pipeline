from unittest.mock import patch,Mock
import pandas as pd

def load_unique_locations(conn, db_update, db_search, df):
    # create a dataframe for the unique locations
    location_df = df["location"].unique()

    # # load the locations into the database
    for location in location_df:
        search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
        values = {"location_name": location.title()}
        result = db_search(conn, search_location, values)
        if result == []:
            sql = "INSERT INTO location (location_name) VALUES (%(location_name)s)"
            values = {"location_name": location.title()}
            db_update(conn, sql, values)

# test 1
def test_load_unique_locations_already_in():
    
    conn = Mock()
    
    mock_db_update = Mock()
    mock_db_update.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = [(1,)]
    
    d = {"location":["Isle of Wight","Isle of Wight"]}
    df = pd.DataFrame(d)
    
    load_unique_locations(conn,mock_db_update,mock_db_search,df)
    
    assert mock_db_search.call_count == 1
    assert mock_db_update.call_count == 0

# test 2
def test_load_unique_locations_not_already_in():
    
    conn = Mock()
    
    mock_db_update = Mock()
    mock_db_update.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = []
    
    d = {"location":["Isle of Wight","Isle of Wight"]}
    df = pd.DataFrame(d)
    
    load_unique_locations(conn,mock_db_update,mock_db_search,df)
    
    assert mock_db_search.call_count == 1
    assert mock_db_update.call_count == 1

# test 3
def test_load_unique_locations_different_locations_already_in():
    
    conn = Mock()
    
    mock_db_update = Mock()
    mock_db_update.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = [(1,)]
    
    d = {"location":["Isle of Wight","Manchester"]}
    df = pd.DataFrame(d)
    
    load_unique_locations(conn,mock_db_update,mock_db_search,df)
    
    assert mock_db_search.call_count == 2
    assert mock_db_update.call_count == 0

# test 4
def test_load_unique_locations_different_locations_not_already_in():
    
    conn = Mock()
    
    mock_db_update = Mock()
    mock_db_update.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = []
    
    d = {"location":["Isle of Wight","Manchester"]}
    df = pd.DataFrame(d)
    
    load_unique_locations(conn,mock_db_update,mock_db_search,df)
    
    assert mock_db_search.call_count == 2
    assert mock_db_update.call_count == 2

test_load_unique_locations_different_locations_not_already_in()

