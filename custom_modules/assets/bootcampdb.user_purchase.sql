CREATE SCHEMA IF NOT EXISTS bootcampdb;
DROP TABLE IF EXISTS bootcampdb.user_purchase;

CREATE TABLE IF NOT EXISTS bootcampdb.user_purchase
(
    id SERIAL PRIMARY KEY,
    InvoiceNo VARCHAR(255),
    StockCode VARCHAR(255),
    Description VARCHAR(255),
    Quantity FLOAT,
    InvoiceDate DATE,
    UnitPrice FLOAT,
    CustomerID FLOAT,
    Country VARCHAR(255) 
);