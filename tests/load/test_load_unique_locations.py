from unittest.mock import Mock, patch
import pandas as pd
import uuid


def load_unique_locations(conn, db_update_many, db_search, df, location_id):
    # create a dataframe for the unique locations
    location_df = df["location"].unique()

    # # load the locations into the database
    sql = "INSERT INTO location (location_id, location_name) VALUES %s"
    args_list = []
    for location in location_df:
        search_location = "SELECT * FROM location WHERE location_name=%(location_name)s"
        values = {"location_name": location.title()}
        result = db_search(conn, search_location, values)
        if result == []:
            location_id = str(uuid.uuid4())
            values = (location_id, location.title())
            args_list.append(values)
    db_update_many(conn, sql, args_list)
    return location_id


# Test 1
def test_load_unique_locations_already_in():
    with patch("uuid.uuid4", return_value="testuuid"):
        d = {"location":["Isle of Wight","Isle of Wight"]}
        df = pd.DataFrame(d)
        
        conn = Mock()
        
        mock_db_update_many = Mock()
        mock_db_update_many.return_value = True
        
        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]
        
        location_id =  None
        
        location_id = load_unique_locations(conn, mock_db_update_many, mock_db_search, df, location_id)
        
        
        assert mock_db_search.call_count == 1
        assert mock_db_update_many.call_count == 1
        # this time no uuid has been created
        assert location_id == None



# Test 2
def test_load_unique_locations_not_already_in():
    with patch("uuid.uuid4", return_value="testuuid"):
        d = {"location":["Isle of Wight","Isle of Wight"]}
        df = pd.DataFrame(d)
        
        conn = Mock()
        
        mock_db_update_many = Mock()
        mock_db_update_many.return_value = True
        
        mock_db_search = Mock()
        mock_db_search.return_value = []
        
        location_id =  None
        
        location_id = load_unique_locations(conn, mock_db_update_many, mock_db_search, df, location_id)
        
        assert mock_db_search.call_count == 1
        assert mock_db_update_many.call_count == 1
        # this time a uuid for the location has been created
        assert location_id == "testuuid"

# test 3
def test_load_unique_locations_different_locations_not_already_in():
    with patch("uuid.uuid4", return_value="testuuid"):
        conn = Mock()
        mock_db_update_many = Mock()
        mock_db_update_many.return_value = True
    
        mock_db_search = Mock()
        mock_db_search.return_value = []
    
        d = {"location":["Isle of Wight","Manchester"]}
        df = pd.DataFrame(d)

        location_id = None
        location_id = load_unique_locations(conn,mock_db_update_many,mock_db_search,df,location_id)
    
        assert mock_db_search.call_count == 2
        assert mock_db_update_many.call_count == 1
        # this time a uuid has been created
        assert location_id == "testuuid"

# test 4
def test_load_unique_locations_different_locations_already_in():
    with patch("uuid.uuid4", return_value="testuuid"):
        conn = Mock()
    
        mock_db_update_many = Mock()
        mock_db_update_many.return_value = True
    
        mock_db_search = Mock()
        mock_db_search.return_value = [(1,)]
    
        d = {"location":["Isle of Wight","Manchester"]}
        df = pd.DataFrame(d)

        location_id = None
        
        location_id = load_unique_locations(conn,mock_db_update_many,mock_db_search,df,location_id)
        
        assert mock_db_search.call_count == 2
        assert mock_db_update_many.call_count == 1
        assert location_id == None
