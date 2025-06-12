from typing import List, Dict, Any

import psycopg2
import requests
from trino.dbapi import Connection


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


def fetch_latest_lauch_data_from_api(url: str):
    try:
        return requests.get(url).json()
    except Exception as e:
        raise Exception(f"API call to {url} unsuccessful! Error: {str(e)}")


def parse_and_validate_api_data(raw_launches: dict):
    mandatory_columns = ['id', 'success']
    for column in mandatory_columns:
        if raw_launches[column] is None:
            raise DataValidationException(f"Column {column} cannot be null!")
    if raw_launches['details']:
        payload_mass = int(raw_launches['details'].split("Total payload mass was ")[-1].split(' kg')[0])
    else:
        payload_mass = 0

    return {
        'id': raw_launches['id'],
        'name': raw_launches['name'],
        'date_unix': raw_launches['date_unix'],
        'success': raw_launches['success'],
        'payload_mass': payload_mass,
        'details': raw_launches['details']
    }


def insert_launches_to_table(table_name: str, launch_data: dict):
    """
    Inserts a single launch record (from a dictionary) into the spacex_launches table.

    Args:
        launch_data (dict): A dictionary representing a single launch record.
        db_params (dict): Dictionary with database connection parameters.
    """
    columns = launch_data.keys()
    values = launch_data.values()
    cols_sql = ", ".join(columns)

    # SQL part for value placeholders: (%s, %s, ...)
    # %s is a placeholder for parameterized queries, which prevents SQL injection
    placeholders = ", ".join(['%s'] * len(values))
    insert_sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders});"

    postgres_connection = None
    try:
        postgres_connection = psycopg2.connect(**DB_CONFIG)
        cursor = postgres_connection.cursor()
        cursor.execute(insert_sql, list(values))
        postgres_connection.commit()
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        if postgres_connection:
            postgres_connection.rollback()
        raise Exception(f"Error inserting data, rolling back. Error: {error}")
    finally:
        if postgres_connection:
            postgres_connection.close()

def insert

def aggregate_data(table_name: str, aggregated_table_name: str):
    """
    README: since average is a non-linear metric it would need to be re-calculated with each row insertion. I would
    deal with this in the following way:
        - For linear metrics I would add the new values to the relevant rows (only the ones which should be updated)
        - For non-linear metrics I would re-calculate the metric using the launches non-aggregated table and then insert
            the values to the relevant rows.

    Generate an aggregated table in PostgreSQL with metrics like:
        o Total launches
        o Total successful launches
        o Average payload mass
        o Average delay between scheduled and actual launch times
    The aggregation logic should be in Python or SQL and kept up to date when new data is  ingested.
    """


    trino_sql_query = f"""
            SELECT
                EXTRACT(YEAR FROM date_utc) AS aggregation_year,
                COUNT(id) AS total_launches,
                COUNT(CASE WHEN success = TRUE THEN id END) AS total_successful_launches,
                AVG(total_payload_mass) AS average_payload_mass
            FROM
                {table_name}
            WHERE
                date_utc IS NOT NULL
            GROUP BY
                EXTRACT(YEAR FROM date_utc)
            ORDER BY
                aggregation_year
            """
    aggregated_data = fetch_from_trino(trino_sql_query)


def fetch_from_trino(query: str, host: str = 'localhost', port: int = 8080, user: str = 'trino',
                                           catalog: str = 'postgresql', schema: str = 'public'):
    conn = None
    cur = None
    aggregated_data = []
    try:
        conn = Connection(host=host, port=port, user=user, catalog=catalog, schema=schema)
        cur = conn.cursor()
        cur.execute(query)

        # fetchall() gets all rows as a list of tuples
        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        for row in rows:
            aggregated_data.append(dict(zip(column_names, row)))

    except Exception as e:
        print(f"Error querying Trino: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return aggregated_data


if __name__ == "__main__":
    """
    Assumptions:
        - All "details" messages are the same structure, when exist. I am disregarding successful launches with None details in the mass average.
    """
raw_launches = fetch_latest_lauch_data_from_api('https://api.spacexdata.com/v5/launches/latest')
launches = parse_and_validate_api_data(raw_launches)
insert_launches_to_table(FACT_TABLE_NAME, launches)  # append-only, incremental ingestion

aggregated_df = aggregate_data(AGGREGATED_TABLE_NAME, launches)
