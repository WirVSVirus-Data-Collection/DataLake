from abc import ABC, abstractmethod
import requests

import pandas as pd


class DataSource(ABC):
    def __init__(
        self, url, endpoint="", info="", date_columns=None, date_format="%d.%m.%Y",
    ):
        self.url = url
        self.endpoint = endpoint
        self.info = info
        if date_columns is None:
            self.date_columns = []
        else:
            self.date_columns = date_columns
        self.date_format = date_format

    @abstractmethod
    def get_data(self):
        pass

    def _ensure_datetime(self, df):
        for c in self.date_columns:
            df[c] = pd.to_datetime(df[c], format=self.date_format)
        return df


class JsonDataSource(DataSource):
    def _get_json(self):
        response = requests.get(self.url + "/" + self.endpoint)
        response.raise_for_status()
        return response.json()

    def _flatten_json(self, json_data):
        return json_data

    def get_data(self):
        df = pd.DataFrame(self._flatten_json(self._get_json()))
        return self._ensure_datetime(df)


class HamburgClinics(JsonDataSource):
    def __init__(self):
        super(HamburgClinics, self).__init__(
            url="https://opendata.arcgis.com/",
            endpoint="datasets/78dc2cd921114c839a21aa8ed48760bc_0.geojson",
            info="Capacity of clinics Hamburg",
            date_columns=["stand"],
        )

    def _flatten_json(self, json_data):
        flattened_data = list()
        for original_row in json_data["features"]:
            final_row = original_row["properties"]
            coordinates = original_row["geometry"]["coordinates"]
            final_row["latitude"] = coordinates[0]
            final_row["longitude"] = coordinates[1]
            flattened_data.append(final_row)
        return flattened_data
