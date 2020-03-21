import json
import pandas as pd

with open('../Krankenhauser__Hamburg.geojson') as file:
    data = pd.read_json(file)

header_keys = ['X', 'Y']
property_keys = list(data['features'][0]['properties'].keys())
header_keys.extend(property_keys)

table = []
for row in data.iterrows():
    temp = row[1]['features']['geometry']['coordinates']
    temp.extend([row[1]['features']['properties'][key] for key in property_keys])
    table.append(temp)

new_df = pd.DataFrame(table, columns=header_keys)
print(new_df.head())

#print(header_keys)
#for row in table[:5]:
#    print(row)

with open('../Krankenhauser__Hamburg.csv', 'r') as f:
    data_csv = pd.read_csv(f)

print(data_csv.head())
