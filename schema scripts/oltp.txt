DROP TABLE IF EXISTS customer_master;
DROP TABLE IF EXISTS product_master;
DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS order_items;


create table customer_master( 
customerid integer(6) Primary key,
name varchar(23) NOT NULL,
address varchar(45) NOT NULL,
city varchar(23) NOT NULL,
state varchar(23) NOT NULL,
pincode integer(6) NOT NULL,
update_timestamp TIMESTAMP NOT NULL);


create table product_master( 
productid integer(6) Primary key,
productcode varchar(23) NOT NULL,
productname varchar(45) NOT NULL,
sku varchar(23) NOT NULL,
rate integer(23) NOT NULL,
isactive BOOLEAN NOT NULL);

create table order_details( 
orderid integer(6) ,
customerid integer(6) ,
order_status_update_timestamp timestamp NOT NULL,
order_status varchar(19) NOT NULL,
FOREIGN KEY (customerid) REFERENCES customer_master (customerid),
primary key(orderid, order_status)
);


create table order_items( 
orderid integer(6),
productid integer(6) NOT NULL,
quantity integer(6) NOT NULL,
primary key(orderid, productid)
);