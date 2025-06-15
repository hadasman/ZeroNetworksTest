class DataValidationException(Exception):
    pass


DB_CONFIG = {
    "host": "localhost",
    "database": "sampledb",
    "user": "trino",
    "password": "trino",
    "port": "5432"
}
FACT_TABLE_NAME = 'spacex_launches'
AGGREGATED_TABLE_NAME = 'agg_spacex_launches'
