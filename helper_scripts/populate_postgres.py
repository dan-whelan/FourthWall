#!/usr/bin/env python
from utils import (
    get_num, get_street, get_city, get_country, 
    get_age, get_name, get_postal_code, 
    insert_list, get_length_of_address_table
    )

if __name__ == '__main__':
    addresses = []
    students = []
    num_rows = 2000
    num_address_rows = get_length_of_address_table()
    start = num_address_rows+1
    for index in range(start, start+num_rows):
        addresses.append((f'{get_num()} {get_street()}', f'{get_city()}', f'{get_country()}', f'{get_postal_code()}'))
        students.append((f'{get_name()}', f'{get_age()}', f'{index}'))

    insert_list(schema = 'college', table = 'address', cols = ('address', 'city', 'country', 'postal_code'), data = addresses)
    print("Successfully inserted addresses.")
    insert_list(schema = 'college', table = 'student', cols = ('name', 'age', 'address_id'), data = students)
    print("Successfully inserted students.")
