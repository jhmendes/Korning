-- DEFINE YOUR DATABASE SCHEMA HERE
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS frequencies;


CREATE TABLE employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255)
);

CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  customer_name VARCHAR(255),
  account_no VARCHAR(255)
);

CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255)
);

CREATE TABLE frequencies (
  id SERIAL PRIMARY KEY,
  frequency VARCHAR(255)
);

CREATE TABLE sales (
  id SERIAL PRIMARY KEY,
  sale_date VARCHAR(255),
  sale_amount MONEY,
  units_sold INTEGER,
  invoice_number INTEGER,
  employee_id  INTEGER REFERENCES employees(id),
  customer_and_account_no_id INTEGER REFERENCES customers(id),
  product_id INTEGER REFERENCES products(id),
  frequency_id INTEGER REFERENCES frequencies(id)
);
