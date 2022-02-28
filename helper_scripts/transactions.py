#!/usr/bin/env python
from utils import connect, disconnect

update_addresses_transaction = """
BEGIN;
UPDATE college.address SET address = '100 Herbert Park'
    WHERE address = '284 Herbert Park';
UPDATE college.address SET address = '100 Iveragh Road'
    WHERE address = '600 Iveragh Road';
COMMIT;
"""

if __name__ == '__main__':
    try:
        conn = connect()
        cursor = conn.cursor()
        cursor.execute(update_addresses_transaction)
        conn.commit()
        print("Adresses successfully changed.")
        cursor.close()
    except Exception as e:
        print(e)
    finally:
        disconnect(conn)
