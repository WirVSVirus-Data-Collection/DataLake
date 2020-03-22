from abc import ABC, abstractmethod
import requests

import io
import pandas as pd
import boto3

BUCKET_NAME_LANDING_ZONE = "wirvsvirus-data-lake-landing-zone"


class DatasetKinds(object):
    health_care_capacity = "health_care_capacity"
    infection_cases = "infection_cases"


class DataSource(ABC):
    def __init__(
        self,
        name,
        kind,
        url,
        endpoint="",
        info="",
        date_columns=None,
        date_format="%d.%m.%Y",
    ):
        self.name = name
        self.kind = kind
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
            df[c] = pd.to_datetime(
                df[c], infer_datetime_format=True, format=self.date_format
            )
        return df

    def write_to_s3(self, s3_client, bucket=BUCKET_NAME_LANDING_ZONE):
        df = self.get_data()
        csv_string = df.to_csv(sep=",", index=False)
        folder_name = self.kind + "__" + self.name.replace(" ", "_")
        key = folder_name + "/data.csv"
        response = s3_client.put_object(Bucket=bucket, Key=key, Body=csv_string)
        return response


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
            name="hamburg_clinics",
            kind=DatasetKinds.health_care_capacity,
            url="https://opendata.arcgis.com/",
            endpoint="datasets/78dc2cd921114c839a21aa8ed48760bc_0.geojson",
            info="Capacity of clinics Hamburg 2016",
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


class CsvDataSource(DataSource):
    def _get_csv(self):
        response = requests.get(self.url + "/" + self.endpoint)
        response.raise_for_status()
        decoded_content = response.content.decode()
        return decoded_content

    def get_data(self):
        df = pd.read_csv(io.StringIO(self._get_csv()), sep=",")
        return self._transform_data(df)


class RKIDataAgeGroupCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataAgeGroupCsv, self).__init__(
            name="RKI_age_group_level",
            kind=DatasetKinds.infection_cases,
            url="https://opendata.arcgis.com",
            endpoint="datasets/dd4580c810204019a7b8eb3e0b329dd6_0.csv",
            info="Data from the Robert-Koch-Institut on the new cases per day. Sorted by gender, age group and county in Germany.",
            date_columns=["Datenstand", "Meldedatum"],
        )

    def _data_cleansing(self, df):
        return df


class RKIDataCountyCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataCountyCsv, self).__init__(
            name="RKI_county_level",
            kind=DatasetKinds.infection_cases,
            url="https://opendata.arcgis.com",
            endpoint="datasets/917fc37a709542548cc3be077a786c17_0.csv",
            info="Data from the Robert-Koch-Institut on the current cases per county.",
        )

    def _data_cleansing(self, df):
        # data of mostly bureaucratic nature, hard to decipher its meaning, likely not relevant for corona-virus
        # instead of subselecting actually needed data (thereby silently removing data), these columns are removed
        df.drop(
            labels=[
                "OBJECTID",
                "ADE",
                "GF",
                "BSG",
                "RS",
                "AGS",
                "SDV_RS",
                "BEZ",
                "IBZ",
                "BEM",
                "NBD",
                "SN_L",
                "SN_R",
                "SN_K",
                "SN_V1",
                "SN_V2",
                "SN_G",
                "FK_S3",
                "NUTS",
                "RS_0",
                "AGS_0",
                "WSK",
                "DEBKG_ID",
                "Shape__Area",
                "Shape__Length",
                "cases_per_population",  # duplicated information
                "BL_ID",
            ],
            axis="columns",
            inplace=True,
        )

        # get the 'SK' or 'LK' for Stadtkreis (city county) or Landkreis (rural county)
        df["county"] = df["county"].str.slice(stop=2)

        df.rename(
            columns={
                "county": "kind_of_county",
                "BL": "federal_state",
                "GEN": "county",
                "cases": "cumulative_cases",
                "EWZ": "population",
                "KFL": "county_area_km2",
            },
            inplace=True,
        )
        return df


class RKIDataStateCsv(CsvDataSource):
    def __init__(self):
        super(RKIDataStateCsv, self).__init__(
            name="RKI_state_level",
            kind=DatasetKinds.infection_cases,
            url="https://opendata.arcgis.com",
            endpoint="datasets/ef4b445a53c1406892257fe63129a8ea_0.csv",
            info="Accumulated cases in federal states in Germany as per Robert-Koch-Institut.",
            date_columns=["Aktualisierung"],
        )


def lambda_handler(message, context):
    s3_client = boto3.client("s3")
    datasets = [
        HamburgClinics(),
        RKIDataAgeGroupCsv(),
        RKIDataStateCsv(),
        RKIDataCountyCsv(),
    ]
    for item in datasets:
        response = item.write_to_s3(s3_client)
    return response
