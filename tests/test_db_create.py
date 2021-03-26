from unittest.mock import Mock, patch

def db_create(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    finally:
        pass

def test_db_create():
    sql = "hola"
    conn = Mock()
    
    db_create(conn,sql)
    
    assert conn.cursor.call_count == 1
    assert conn.commit.call_count == 1
