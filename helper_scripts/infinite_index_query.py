#!/usr/bin/env python
from utils import connect, disconnect

if __name__ == '__main__':
    sql1 = "SELECT * FROM college.address where city = 'London';"
    sql2 = "SELECT * FROM college.address where country = 'Oman';"
    while True:
        try:
            conn = connect()
            cursor = conn.cursor()
            cursor.execute(sql1)
            conn.commit()
            print(cursor.fetchall(), "\n")
            cursor.execute(sql2)
            conn.commit()
            print(cursor.fetchall())
            print("succesful")
            cursor.close()
        except Exception as e:
            print("error")
            print(e)
        finally:
            disconnect(conn)
