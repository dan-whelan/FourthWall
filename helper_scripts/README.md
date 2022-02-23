# Scripts for Testing With a Mock Database

## How to Use

- Download Postgresql 14.
- Create a Database called finbourne (it can be any name really but be sure to replace the database name in utils.py).
- In the helper_scripts folder create a file called ".env" and write ```PASSWORD=yourPostgresPassword```.
- Run create_tables.py to create two tables in a specified schema (can only be run once).
- Run populate_postgres.py (can be run any number of times, by default it adds 10 rows to the two tables created in the previous step on each run but you can modify this number).
- Run create_index.py to create two indexes on the college.address table created in step 4.

## Note

- You can modify the files locally or add according to your use case.
- You can use pqsl to test out some of the queries in api_queries.py or general queries like using ```SELECT * FROM college.student;``` to view the college.student table for example.

## Extra Requirements

- Python 3.9+
