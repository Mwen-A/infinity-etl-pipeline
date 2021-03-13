-- the first thing to do in adminer is to create a database called "our database"

/* COLLECTION OF DB QUERIES */
CREATE SEQUENCE location_location_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;

/* for creating the location table */
CREATE TABLE "public"."location" (
    "location_id" integer DEFAULT nextval('location_location_id_seq') NOT NULL,
    "location_name" character varying NOT NULL,
    CONSTRAINT "location_location_id" PRIMARY KEY ("location_id")
) WITH (oids = false);

/* for creating the product table */
CREATE SEQUENCE product_product_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;

CREATE TABLE "public"."product" (
    "product_id" integer DEFAULT nextval('product_product_id_seq') NOT NULL,
    "product_name" character varying NOT NULL,
    "product_size" character varying NOT NULL,
    CONSTRAINT "product_product_id" PRIMARY KEY ("product_id")
) WITH (oids = false);

/* For creating the purchase table */
CREATE SEQUENCE purchase_purchase_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;

CREATE TABLE "public"."purchase" (
    "purchase_id" integer DEFAULT nextval('purchase_purchase_id_seq') NOT NULL,
    "total_price" money NOT NULL,
    "payment_type" character varying NOT NULL,
    "purchase_time" timestamp NOT NULL,
    "location_id" integer NOT NULL,
    CONSTRAINT "purchase_purchase_id" PRIMARY KEY ("purchase_id"),
    CONSTRAINT "purchase_location_id_fkey" FOREIGN KEY (location_id) REFERENCES location(location_id) NOT DEFERRABLE
) WITH (oids = false);

/* For creating the transaction table */
CREATE SEQUENCE transaction_transaction_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1;

CREATE TABLE "public"."transaction" (
    "transaction_id" integer DEFAULT nextval('transaction_transaction_id_seq') NOT NULL,
    "product_id" integer NOT NULL,
    "purchase_id" integer NOT NULL,
    "transaction_price" money NOT NULL,
    CONSTRAINT "transaction_transaction_id" PRIMARY KEY ("transaction_id"),
    CONSTRAINT "transaction_product_id_fkey" FOREIGN KEY (product_id) REFERENCES product(product_id) NOT DEFERRABLE,
    CONSTRAINT "transaction_purchase_id_fkey" FOREIGN KEY (purchase_id) REFERENCES purchase(purchase_id) NOT DEFERRABLE
) WITH (oids = false);