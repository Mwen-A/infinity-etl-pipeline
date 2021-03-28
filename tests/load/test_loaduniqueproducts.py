from unittest.mock import patch, Mock
import pytest

def load_unique_products(conn, db_update_many, db_search, products_set):
    # load the unique products into the database
    args_list = []
    sql = "INSERT INTO product (product_id, product_name, product_size) VALUES %s"
    for prod_id, size, name in products_set:
        search_product = "SELECT * FROM product WHERE product_size=%(product_size)s AND product_name=%(product_name)s"
        values = {"product_id": prod_id, "product_name": name, "product_size": size}
        result = db_search(conn, search_product, values)
        if result == []:
            values = (prod_id, name, size)
            args_list.append(values)
    db_update_many(conn, sql, args_list)


def test_load_unique_products_two_products():
    
    mock_conn = Mock()
    
    mock_db_update_many = Mock()
    mock_db_update_many.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = True
    
    mock_products_set = [[1,2,3],[4,5,6]]
    
    load_unique_products(mock_conn, mock_db_update_many, mock_db_search, mock_products_set) 
    assert mock_db_search.call_count == 2
    assert mock_db_update_many.call_count == 1

def test_load_unique_products_no_products():
    
    mock_conn = Mock()
    
    mock_db_update_many = Mock()
    mock_db_update_many.return_value = True
    
    mock_db_search = Mock()
    mock_db_search.return_value = True
    
    mock_products_set = []
    
    load_unique_products(mock_conn, mock_db_update_many, mock_db_search, mock_products_set) 
    assert mock_db_search.call_count == 0
    assert mock_db_update_many.call_count == 1

def test_load_unique_products_invalid_products():
    with pytest.raises(TypeError):
        mock_conn = Mock()
        
        mock_db_update_many = Mock()
        mock_db_update_many.return_value = True
        
        mock_db_search = Mock()
        mock_db_search.return_value = True
        
        mock_products_set = [1,2]
        
        load_unique_products(mock_conn, mock_db_update_many, mock_db_search, mock_products_set) 
        
        assert mock_db_search.call_count == 0
        assert mock_db_update_many.call_count == 0

