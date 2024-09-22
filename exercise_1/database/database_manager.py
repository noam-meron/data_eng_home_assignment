import logging
import sqlite3
from typing import List, Dict

import pandas as pd

from exercise_1.utils.get_file_path import get_file_path
from exercise_1.utils.query_enums import QueryName


class DatabaseManager:
    def __init__(self, db_name: str, sql_filename: str):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        self.queries = self._load_queries(sql_filename)

    @staticmethod
    def _load_queries(sql_filename: str) -> Dict[QueryName, str]:
        file_path = get_file_path(sql_filename, 'sql')
        queries = {}
        current_query = []
        current_name = None

        with open(file_path, 'r') as file:
            for line in file:
                if line.strip().startswith('--'):
                    if current_query:
                        queries[current_name] = ' '.join(current_query)
                        current_query = []
                    current_name = QueryName[line.strip()[2:].strip().replace(' ', '_')]
                else:
                    current_query.append(line.strip())

        if current_query:
            queries[current_name] = ' '.join(current_query)

        return queries

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to database: {self.db_name}")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to database: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def fetch_all(self, query_name: QueryName, params: tuple = None) -> List[tuple]:
        try:
            query = self.queries.get(query_name)
            if not query:
                raise ValueError(f"Query '{query_name}' not found")

            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error fetching data for query '{query_name}': {e}")
            raise

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            return

        try:
            df.to_sql(table_name, self.conn, if_exists='replace', index=False)
        except sqlite3.Error as e:
            logging.error(f"Error inserting data into {table_name}: {e}")
            self.conn.rollback()
            raise

    @staticmethod
    def write_to_csv(df: pd.DataFrame, filename: str):
        file_path = get_file_path(filename, 'data/processed')
        try:
            df.to_csv(file_path, index=False)
            logging.info(f"Data written to {filename}")
        except Exception as e:
            logging.error(f"Error writing to CSV: {e}")
            raise


db_manager = DatabaseManager('mediation_data.db', 'queries.sql')