#!/usr/bin/env python3

import json
import pandas as pd
import sys

def csv_reader():
    with open('../../Krankenhauser__Hamburg.csv', 'r') as fname:
        table = pd.read_csv(fname)

    print(table.head)
    return table


def json_reader():
    with open('../../Krankenhauser__Hamburg.geojson') as file:
        data = json.load(file)

    print(data['features'][0].keys())
    #print(data['type'])
    return data
