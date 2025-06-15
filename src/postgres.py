import psycopg2

from src.global_variables import DB_CONFIG


class Postgres():
    def __init__(self):
        self.postgres_connection = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.postgres_connection.cursor()

    def insert(self, table_name: str, column_names: list, values: list):
        """
        SQL part for value placeholders: (%s, %s, ...). Useful for parameterized queries to prevent SQL injection.
        """
        insert_sql = self._get_insert_query(table_name, column_names, values)
        self._execute_query(insert_sql, values)

    def upsert(self, table_name: str, column_names: list, values: list, primary_key_name: str):
        """
        SQL part for value placeholders: (%s, %s, ...). Useful for parameterized queries to prevent SQL injection.
        """
        upsert_sql = self._get_upsert_query(table_name, column_names, values, primary_key_name)
        self._execute_query(upsert_sql, values, batch=True)

    def _get_upsert_query(self, table_name: str, column_names: list, values: list, primary_key_name: str):
        insert_sql = self._get_insert_query(table_name, column_names, values).replace(";", "")
        update_sql = ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names if col != primary_key_name])
        return f"""{insert_sql} ON CONFLICT ({primary_key_name}) DO UPDATE SET {update_sql};"""

    @staticmethod
    def _get_insert_query(table_name: str, column_names: list, values: list):
        cols_sql = ", ".join(column_names)
        placeholders = ", ".join(['%s'] * len(column_names))
        return f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders});"

    def _execute_query(self, query: str, args: list, batch: bool = False):
        try:
            if batch:
                self.cursor.executemany(query, args)
            else:
                self.cursor.execute(query, args)
            self.postgres_connection.commit()
        except (Exception, psycopg2.Error) as error:
            if self.postgres_connection:
                self.postgres_connection.rollback()
            raise Exception(f"Error inserting data, rolling back. Error: {error}")
