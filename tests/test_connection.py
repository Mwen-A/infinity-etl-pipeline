from unittest.mock import Mock, patch
import psycopg2

HOST = "host"
USER = "user"
PASSWORD = "password"
DB = "db"
PORT = "port"


def connection():
    return psycopg2.connect(
        host=HOST, user=USER, password=PASSWORD, dbname=DB, port=PORT
    )

def test_connection():
    with patch("psycopg2.connect", return_value=True):
        
        result = connection()
        
        assert result == True