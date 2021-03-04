CREATE DATABASE our_database
OWNER = postgres
TEMPLATE = template 
ENCODING = encoding 
LC_COLLATE = collate 
LC_CTYPE = ctype
TABLESPACE = tablespace_name 
CONNECTION LIMIT = max_concurrent_connection

CREATE TABLE product(  
product_id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
product_name VARCHAR(200) NOT NULL,
product_size VARCHAR(10) NOT NULL,  
PRIMARY KEY(product_id)
);  

CREATE TABLE Location(
   Location_id INT NOT NULL,
   Location_name VARCHAR(50) NOT NULL,
   PRIMARY KEY(Location_id)
);

CREATE TABLE Purchase(  
Purchase_id INT GENERATED ALWAYS AS IDENTITY NOT NULL,  
Total_price money NOT NULL,
Payment_type VARCHAR(200) NOT NULL,
Purchase_time TIMESTAMP NOT NULL, 
Location_id INT NOT NULL,
PRIMARY KEY(purchase_id),  
FOREIGN KEY(location_id),  
REFERENCES Location (Location_id)
); 

CREATE TABLE Transaction(  
Transction_id INT GENERATED ALWAYS AS IDENTITY NOT NULL,  
Product_id INT NOT NULL, 
Purchase_id INT NOT NULL,  
Transaction_price money NOT NULL,
PRIMARY KEY(Tranaction_id),  
FOREIGN KEY(Product_id) 
REFERENCES Product (Product_id)
FOREIGN KEY(Purchase_id)
REFERENCES Purchase (Purchase_id) 
); 






