from abc import ABC, abstractmethod
import requests
import io
import pandas as pd
import re


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

    def _data_cleansing(self, df):
        return df

    def _transform_data(self, df):
        df = self._data_cleansing(df)
        return self._ensure_datetime(df)

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
        return self._transform_data(df)


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


class RKIDataAgeGroupJson(JsonDataSource):
    def __init__(self):
        super(RKIDataAgeGroupJson, self).__init__(
            url="https://opendata.arcgis.com",
            endpoint="datasets/dd4580c810204019a7b8eb3e0b329dd6_0.geojson",
            info="Data from the Robert-Koch-Institut on the new cases per day. Sorted by gender, age group and county in Germany.",
        )

    def _flatten_json(self, json_data):
        flattened_data = []
        for dict_row in json_data['features']:
            # value of 'properties' is itself a dictionary
            # a list of dictionaries can easily be put into a pandas.DataFrame
            flattened_data.append(dict_row['properties'])
        return flattened_data


class RKIDataCountyJson(JsonDataSource):
    def __init__(self):
        super(RKIDataCountyJson, self).__init__(
            url="https://opendata.arcgis.com",
            endpoint="datasets/917fc37a709542548cc3be077a786c17_0.geojson",
            info="Data from the Robert-Koch-Institut on the current cases per county.",
        )

    def _flatten_json(self, json_data):
        flattened_data = []
        for dict_row in json_data['features']:
            # value of 'properties' is itself a dictionary
            # a list of dictionaries can easily be put into a pandas.DataFrame
            flattened_data.append(dict_row['properties'])
        return flattened_data


class RKIDataStateJson(JsonDataSource):
    def __init__(self):
        super(RKIDataStateJson, self).__init__(
            url="https://opendata.arcgis.com",
            endpoint="datasets/ef4b445a53c1406892257fe63129a8ea_0.geojson",
            info="Accumulated cases in federal states in Germany as per Robert-Koch-Institut.",
        )

    def _flatten_json(self, json_data):
        flattened_data = []
        for dict_row in json_data['features']:
            # value of 'properties' is itself a dictionary
            # a list of dictionaries can easily be put into a pandas.DataFrame
            flattened_data.append(dict_row['properties'])
        return flattened_data


class CsvDataSource(DataSource):
    def _get_csv(self):
        response = requests.get(self.url + "/" + self.endpoint)
        response.raise_for_status()
        decoded_content = response.content.decode()
        return decoded_content

    def get_data(self):
        df = pd.read_csv(io.StringIO(self._get_csv()), sep=',')
        return self._transform_data(df)


class RKIDataAgeGroupCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataAgeGroupCsv, self).__init__(
                url="https://opendata.arcgis.com",
                endpoint="datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv",
                info="Data from the Robert-Koch-Institut on the new cases per day. Sorted by gender, age group and county in Germany.",
            )
    def _data_cleansing(self, df):
        return df


class RKIDataCountyCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataCountyCsv, self).__init__(
                url="https://opendata.arcgis.com",
                endpoint="datasets/917fc37a709542548cc3be077a786c17_0.csv",
                info="Data from the Robert-Koch-Institut on the current cases per county.",
            )


class RKIDataStateCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataStateCsv, self).__init__(
            url="https://opendata.arcgis.com",
            endpoint="datasets/ef4b445a53c1406892257fe63129a8ea_0.csv",
            info="Accumulated cases in federal states in Germany as per Robert-Koch-Institut.",
        )
