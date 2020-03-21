# DataLake

#DataHygiene
    tba

#DataScraper

The data scraper will download data of various kinds from different sources, i.e. the kind
'infection_rate_state_wise' may be available from the Robert Koch institute as well as from
John Hopkins University. The data is returned as flat `pandas.DataFrame`s.

## Usage

```
    >>>from DatalLake.DataScraper import DataScraper
    >>>my_scraper = DataScraper()
    >>>my_scraper.infection_rate_state_wise.john_hopkins_university.url
    'https://fakeurl.com'
    >>>my_scraper.infection_rate_state_wise.john_hopkins_university.info
    'Some info about this dataset'
    >>>my_dataframe = my_scraper.infection_rate_state_wise.john_hopkins_university.get_data()
```

The current assumption is that each dataset can be represented as one flat table, without blowing
up the data too much.




