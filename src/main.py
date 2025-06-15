import requests
from trino.dbapi import Connection

from src.global_variables import DataValidationException, FACT_TABLE_NAME, AGGREGATED_TABLE_NAME
from src.postgres import Postgres


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

    engine_start_time = raw_launches['static_fire_date_unix']
    launch_time = raw_launches['date_unix']
    if engine_start_time:
        launch_delay = abs(raw_launches['date_unix'] - engine_start_time)
    else:
        launch_delay = 0

    return {
        'id': raw_launches['id'],
        'name': raw_launches['name'],
        'launch_date_unix': launch_time,
        'success': raw_launches['success'],
        'payload_mass': payload_mass,
        'details': raw_launches['details'],
        'engine_start_time_unix': engine_start_time,
        'launch_delay_hours': round(launch_delay / 3600)
    }


def insert_launches_to_table(table_name: str, launch_data: dict, postgres: Postgres):
    """
    Inserts a single launch record (from a dictionary) into the spacex_launches table.

    Args:
        launch_data (dict): A dictionary representing a single launch record.
        table_name (str): table name into which to insert the data.
    """
    columns = launch_data.keys()
    values = launch_data.values()
    postgres.insert(table_name, list(columns), list(values))


def aggregate_data(table_name: str, aggregated_table_name: str, postgres: Postgres):
    """
    README:
    - Aggregation is on the year part of launch date
    - Since average is a non-linear metric it would need to be re-calculated with each row insertion. I would
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

    aggregation_logic = "EXTRACT(YEAR FROM FROM_UNIXTIME(launch_date_unix))"
    agg_column_name = "aggregation_year"
    aggregation_query = f"""
            SELECT
                {aggregation_logic} AS {agg_column_name},
                COUNT(id) AS total_launches,
                COUNT(CASE WHEN success = TRUE THEN id END) AS total_successful_launches,
                AVG(payload_mass) AS average_payload_mass,
                AVG(launch_delay_hours) AS average_delay_hours
            FROM
                {table_name}
            WHERE
                launch_date_unix IS NOT NULL
            GROUP BY
                {aggregation_logic}
            ORDER BY
                {agg_column_name}
            """
    aggregated_data_rows = fetch_from_trino(aggregation_query)
    column_names = list(aggregated_data_rows[0].keys())
    data_to_insert = []
    for launch in aggregated_data_rows:
        data_to_insert.append([launch.get(col) for col in column_names])

    postgres.upsert(aggregated_table_name, column_names, data_to_insert, agg_column_name)


def fetch_from_trino(query: str, host: str = 'localhost', port: int = 8080, user: str = 'trino',
                     catalog: str = 'postgresql', schema: str = 'public'):
    conn = None
    cur = None
    aggregated_data = []
    try:
        conn = Connection(host=host, port=port, user=user, catalog=catalog, schema=schema)
        cur = conn.cursor()
        cur.execute(query)

        column_names = [desc[0] for desc in cur.description]
        rows = cur.fetchall()  # fetchall() gets all rows as a list of tuples
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
        - Failed launches are also counted in the calculation of mass average
        - If no static_fire_date (sometimes None), delay = 0
    """
    postgres_object = Postgres()

    raw_launches = fetch_latest_lauch_data_from_api('https://api.spacexdata.com/v5/launches/latest')
    launches = parse_and_validate_api_data(raw_launches)
    insert_launches_to_table(FACT_TABLE_NAME, launches, postgres_object)  # append-only, incremental ingestion

    aggregate_data(FACT_TABLE_NAME, AGGREGATED_TABLE_NAME, postgres_object)

    postgres_object.postgres_connection.close()
