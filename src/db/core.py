import os
import psycopg2  # postgreSQL database adapter
import psycopg2.extras
from dotenv import load_dotenv  # for loading env variables from .env file

load_dotenv()

# ---->>> not sure what the name of the db was on adminer was it POSTGRES_DB=project_database??, does this matter?

# DATABASE CONNECTION SETUP

PASSWORD = os.environ.get("POSTGRES_PASSWORD")
HOST = os.environ.get("POSTGRES_HOST")
PORT = int(os.environ.get("POSTGRES_PORT"))
DB = os.environ.get("POSTGRES_DB")
USER = os.environ.get("POSTGRES_USER")


def connection():
    return psycopg2.connect(
        host=HOST, user=USER, password=PASSWORD, dbname=DB, port=PORT
    )


conn = connection()

# or as a function??, with conn.cursor() as cur:

# DATABASE COMMANDS


def db_query(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    finally:
        pass
    # connection close?, catch exceptions..


def db_search(conn, sql, values):
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        return cur.fetchall()
    finally:
        pass


def db_update(conn, sql, values):
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
    finally:
        pass

def db_update_many(conn, sql, values_list):
    try:
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur,sql,values_list)
        conn.commit()
    finally:
        pass


def db_delete(conn, sql, index):
    try:
        cur = conn.cursor()
        cur.execute(sql, index)
        conn.commit()
    finally:
        pass