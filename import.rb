# Use this file to import the sales information into the
# the database.

require "pg"
require 'csv'
require 'pry'

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

system("psql korning < schema.sql")

CSV.foreach('sales.csv',headers: true) do |row|

  db_connection do |conn|

    values = row[0].split(" ")
    name = values[0..1].join(" ")
    email = values[2]

    sql = 'SELECT name FROM employees WHERE name=$1'
    results = conn.exec_params(sql, [name])

    if results.to_a.empty?
      sql = "INSERT INTO employees (name, email) VALUES ($1, $2)"
      results = conn.exec_params(sql, [name, email])
    end

    values = row[1].split(" ")
    customer_name = values[0]
    account_no = values[1]

    sql = 'SELECT customer_name FROM customers WHERE customer_name=$1'
    results = conn.exec_params(sql, [customer_name])

    if results.to_a.empty?
      sql = "INSERT INTO customers (customer_name, account_no) VALUES ($1, $2)"
      results = conn.exec_params(sql, [customer_name, account_no])
    end

    product_name = row[2]

    sql = 'SELECT name FROM products WHERE name=$1'
    results = conn.exec_params(sql, [product_name])

    if results.to_a.empty?
      sql = "INSERT INTO products (name) VALUES ($1)"
      results = conn.exec_params(sql, [product_name])
    end

    frequency = row[-1]

    sql = 'SELECT frequency FROM frequencies WHERE frequency=$1'
    results = conn.exec_params(sql, [frequency])

    if results.to_a.empty?
      sql = "INSERT INTO frequencies (frequency) VALUES ($1)"
      results = conn.exec_params(sql, [frequency])
    end

    sale_date = row['sale_date']
    sale_amount = row['sale_amount']
    units_sold = row['units_sold']
    invoice_no = row['invoice_no']

    sql = 'SELECT id FROM employees WHERE name=$1'
    employee_id = conn.exec_params(sql, [name])[0]['id']

    sql = 'SELECT id FROM customers WHERE customer_name=$1'
    customer_id = conn.exec_params(sql, [customer_name])[0]['id']

    sql = 'SELECT id FROM products WHERE name=$1'
    product_id = conn.exec_params(sql, [product_name])[0]['id']

    sql = 'SELECT id FROM frequencies WHERE frequency=$1'
    frequency_id = conn.exec_params(sql, [frequency])[0]['id']

      sql = "INSERT INTO sales (sale_date, sale_amount, units_sold, invoice_number, employee_id, customer_and_account_no_id, product_id, frequency_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
      results = conn.exec_params(sql, [sale_date, sale_amount, units_sold, invoice_no, employee_id, customer_id, product_id, frequency_id])


    # how can I make the last sql statement shorter/multiple lines instead of one long string?

  end


end
