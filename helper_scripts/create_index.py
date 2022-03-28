#!/usr/bin/env python
from utils import create_index

if __name__ == '__main__':
    for col in ('city', 'country'):
        create_index('college', 'address', f'idx_address_{col}', col)
