#!/usr/bin/env python3

import sys

first_file_fn=sys.argv[1]
second_file_fn=sys.argv[2]

first_list=set()
second_list=set()

with open(first_file_fn) as f1:
    data1 = f1.read().splitlines()
    for stock in data1:
        first_list.add(stock)
with open(second_file_fn) as f1:
    data1 = f1.read().splitlines()
    data2 = [x for x in data1 if x not in first_list]
    for elem in data2:
        print(elem)
