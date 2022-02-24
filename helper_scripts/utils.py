import psycopg2
import os
from dotenv import load_dotenv 
import random

load_dotenv()

def connect():
    conn = psycopg2.connect(f"dbname=finbourne user=postgres password={os.getenv('PASSWORD')}")
    if not conn.closed:
        print("Connected to Postgresql database, 'finbourne'.")
    
    return conn

def disconnect(conn):
    conn.close()
    if conn.closed:
        print("Disconnected from the Postgresql database, 'finbourne'.")

def get_length_of_address_table() -> int:
    sql = "SELECT count(*) FROM college.address;"
    result = None
    try:
        # connect to the PostgreSQL database
        conn = connect()
        # create a new cursor
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        result = result[0]
        # close communication with the database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        disconnect(conn)
    return result 

def unpack(iterable):
    return ", ".join(str(x) for x in iterable)

def insert_list(schema, table, cols: tuple[str], data: list):
    """ insert multiple rows into the table  """
    # this is a quick script to test, using python to format queries can lead to a SQL injection 
    sql = f"INSERT INTO {schema}.{table} ({unpack(cols)}) VALUES ({','.join(['%s']*len(cols))})"
    try:
        # connect to the PostgreSQL database
        conn = connect()
        # create a new cursor
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.executemany(sql, data)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        disconnect(conn)

def create_index(schema, table, indexName, column):
    sql = f"CREATE INDEX {indexName} ON {schema}.{table}({column});"
    print(sql)
    try:
        # connect to the PostgreSQL database
        conn = connect()
        # create a new cursor
        cursor = conn.cursor()
        # execute the INSERT statement
        cursor.execute(sql)
        # commit the changes to the database
        conn.commit()
        print(f"Successfully created the index, {indexName}")
        # close communication with the database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        disconnect(conn)
        

chars = [chr(x) for x in range(65, 91)] + [chr(x) for x in range(48, 58)]
countries = []
cities = []
streets = []
names = []

with open("data/countries.txt") as f:
    countries = list(map(lambda x: x.strip(), f.readlines()))

with open("data/streets.txt") as f:
    streets = list(map(lambda x: x.strip(), f.readlines()))

with open("data/cities.txt") as f:
    cities = list(map(lambda x: x.strip(), f.readlines()))

with open("data/names.txt") as f:
    names = list(map(lambda x: x.strip(), f.readlines()))

def get_num():
    return random.randrange(1, 1001)

def get_street():
    return random.choice(streets)

def get_country():
    return random.choice(countries)

def get_postal_code():
    code = ""
    for _ in range(6):
        code += random.choice(chars)
    return code 

def get_name():
    return random.choice(names)

def get_age():
    return random.randrange(17, 25)

def get_city():
    return random.choice(cities)
