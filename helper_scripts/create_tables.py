#!/usr/bin/env python
from utils import connect, disconnect

college_command = """
CREATE SCHEMA college  
    CREATE TABLE address (
        address_id serial PRIMARY KEY,
        address TEXT NOT NULL,
        address2 TEXT, 
        city TEXT NOT NULL, 
        country TEXT NOT NULL,
        postal_code TEXT 
    )
    CREATE TABLE student (
        student_id serial PRIMARY KEY, 
        name TEXT NOT NULL, 
        age INT NOT NULL,
        address_id INT NOT NULL, 
        FOREIGN KEY (address_id)
            REFERENCES address (address_id)
    );
"""

if __name__ == '__main__':
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(college_command)
        print("Tables created successfully.")
        conn.commit()
        cursor.close()
    except Exception as e:
        print(e)
    finally:
        disconnect(conn)
