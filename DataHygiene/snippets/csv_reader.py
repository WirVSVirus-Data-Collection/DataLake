#!/usr/bin/env python3

import pandas as pd
import sys

with open('../../Krankenhauser__Hamburg.csv', 'r') as fname:
    table = pd.read_csv(fname)

print(table.head)

