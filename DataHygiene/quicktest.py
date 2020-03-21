import json
import pandas as pd

with open('../Krankenhauser__Hamburg.geojson') as file:
    data = json.load(file)