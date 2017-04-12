Information is often transferred between applications in a denormalized format so that it can be stored in a single file. In this challenge you'll receive such a file and define a database schema to store the information in a normalized format and write a script to import data from a CSV.

### Instructions

The *sales.csv* file contains a list of sales from Korning, a company that manufactures highly durable, scratch-resistent glass used by smartphone makers like Motorola, LG, and HTC to name a few.

The CSV file contains the invoices for all major purchases in 2012. Each row represents a sale and contains the employee who was in charge of the sale, the customer, the product being sold, how much was sold, and additional invoice details.

We want to import this information into a database so we can generate reports easily using SQL. Define a database schema that can store the information in the CSV in a normalized fashion (i.e. minimizing the amount of redundant information) and write the schema definition to `schema.sql`.

Then write an importer script that will read the CSV file and populate the database with the information from the CSV. When parsing the CSV, consider what data will be useful to store for each type of information based on how we might want to query it. For example, we may want to query employees by e-mail so we should ensure that our database makes it easy to search just by e-mail address.

When the schema and import script are completed, you should be able to run the following commands to build the database:

```no-highlight
$ createdb korning
$ psql korning < schema.sql
$ ruby import.rb
```

If you want to wipe the database and start from a clean slate, run the `dropdb korning` command first (not recommended for production).

### Bonus Challenge: Idempotence

Idempotence is a property that ensures you can run the same operation multiple times without any additional effects. In this application, you'll want to avoid inserting duplicate information if the importer script is run multiple times.

Change `import.rb` so that it can be run repeatedly and will avoid inserting rows if they've already been added. Consider how you can detect if a row already exists: what are the uniquely identifying aspects of each table?

### A Note on Data Quality

Whether it's a CSV file or a SQL dump, when you receive a data set, it's
important to inspect it for quality. Through working on this challenge, you may
notice that a particular field, which you expected to be unique, is duplicated
across multiple entries. What should you do in this scenario?

If we were hired by Korning to migrate their data, it would be worthwhile to
drop by the sales department and ask about these duplicate entries, and how we
should handle them.

As far as this challenge goes, you may choose to handle these suspicious entries
as you see fit.
