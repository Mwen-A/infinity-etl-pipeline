/* COLLECTION OF DB QUERIES */
CREATE TABLE "public"."location" (
    "location_id" uuid NOT NULL,
    "location_name" character varying NOT NULL,
    CONSTRAINT "location_location_id" PRIMARY KEY ("location_id")
) WITH (oids = false);


CREATE TABLE "public"."product" (
    "product_id" uuid NOT NULL,
    "product_name" character varying NOT NULL,
    "product_size" character varying NOT NULL,
    CONSTRAINT "product_product_id" PRIMARY KEY ("product_id")
) WITH (oids = false);


CREATE TABLE "public"."purchase" (
    "purchase_id" uuid NOT NULL,
    "total_price" money NOT NULL,
    "payment_type" character varying NOT NULL,
    "purchase_time" timestamp NOT NULL,
    "location_id" uuid NOT NULL,
    CONSTRAINT "purchase_purchase_id" PRIMARY KEY ("purchase_id"),
    CONSTRAINT "purchase_location_id_fkey" FOREIGN KEY (location_id) REFERENCES location(location_id) NOT DEFERRABLE
) WITH (oids = false);


CREATE TABLE "public"."transaction" (
    "transaction_id" uuid NOT NULL,
    "product_id" uuid NOT NULL,
    "purchase_id" uuid NOT NULL,
    "transaction_price" money NOT NULL,
    CONSTRAINT "transaction_transaction_id" PRIMARY KEY ("transaction_id"),
    CONSTRAINT "transaction_product_id_fkey" FOREIGN KEY (product_id) REFERENCES product(product_id) NOT DEFERRABLE,
    CONSTRAINT "transaction_purchase_id_fkey" FOREIGN KEY (purchase_id) REFERENCES purchase(purchase_id) NOT DEFERRABLE
) WITH (oids = false);