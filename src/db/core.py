import os
import psycopg2  # postgreSQL database adapter
import psycopg2.extras
from dotenv import load_dotenv  # for loading env variables from .env file

load_dotenv()

# DATABASE CONNECTION SETUP

PASSWORD = os.environ.get("DB_PASSWORD")
HOST = os.environ.get("DB_HOST")
PORT = int(os.environ.get("DB_PORT"))
DB = os.environ.get("DB")
USER = os.environ.get("DB_USER")


def connection():
    return psycopg2.connect(
        host=HOST, user=USER, password=PASSWORD, dbname=DB, port=PORT
    )


conn = connection()

# DATABASE CONN FUNCTIONS

def db_create(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
    finally:
        pass


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

create_transaction = '''
CREATE TABLE IF NOT EXISTS "public"."transaction" (
    "transaction_id" uuid NOT NULL,
    "product_id" uuid NOT NULL,
    "purchase_id" uuid NOT NULL,
    "transaction_price" money NOT NULL,
    CONSTRAINT "transaction_transaction_id" PRIMARY KEY ("transaction_id"),
    CONSTRAINT "transaction_product_id_fkey" FOREIGN KEY (product_id) REFERENCES product(product_id) NOT DEFERRABLE,
    CONSTRAINT "transaction_purchase_id_fkey" FOREIGN KEY (purchase_id) REFERENCES purchase(purchase_id) NOT DEFERRABLE
) WITH (oids = false);
'''

create_purchase = '''
CREATE TABLE IF NOT EXISTS "public"."purchase" (
    "purchase_id" uuid NOT NULL,
    "total_price" money NOT NULL,
    "payment_type" character varying NOT NULL,
    "purchase_time" timestamp NOT NULL,
    "location_id" uuid NOT NULL,
    CONSTRAINT "purchase_purchase_id" PRIMARY KEY ("purchase_id"),
    CONSTRAINT "purchase_location_id_fkey" FOREIGN KEY (location_id) REFERENCES location(location_id) NOT DEFERRABLE
) WITH (oids = false);
'''

create_product = '''
CREATE TABLE IF NOT EXISTS "public"."product" (
    "product_id" uuid NOT NULL,
    "product_name" character varying NOT NULL,
    "product_size" character varying NOT NULL,
    CONSTRAINT "product_product_id" PRIMARY KEY ("product_id")
) WITH (oids = false);
'''

create_location = '''
CREATE TABLE IF NOT EXISTS "public"."location" (
    "location_id" uuid NOT NULL,
    "location_name" character varying NOT NULL,
    CONSTRAINT "location_location_id" PRIMARY KEY ("location_id")
) WITH (oids = false);
'''

# drop table location cascade;
# drop table product cascade;
# drop table purchase cascade;
# drop table transaction cascade;

# drop sequence location_location_id_seq cascade;
# drop sequence transaction_transaction_id_seq cascade;
# drop sequence product_product_id_seq cascade;
# drop sequence purchase_purchase_id_seq cascade;