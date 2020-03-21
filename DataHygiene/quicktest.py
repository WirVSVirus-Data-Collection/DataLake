import json
import pandas as pd

with open('../Krankenhauser__Hamburg.geojson') as file:
    data = json.load(file)

print(data['features'][0].keys())
#print(data['type'])
