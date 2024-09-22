from enum import Enum
from typing import Iterator

import pandas as pd

from exercise_1.database.database_manager import DatabaseManager
from exercise_1.utils.get_file_path import get_file_path
from exercise_1.utils.query_enums import QueryName


class TableName(Enum):
    MEDIATION_DATA = 'mediation_raw_data'
    APPLICATIONS = 'applications'


class DataProcessor:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self._mediation_raw_file_path = get_file_path('mediation_raw_data.csv', 'data/raw')
        self._applications_file_path = get_file_path('applications.csv', 'data/raw')

    def aggregate_data(self) -> pd.DataFrame:
        result = self.db_manager.fetch_all(QueryName.AGGREGATE_DATA)
        columns = ['event_date', 'application_id', 'country', 'total_impressions', 'total_clicks', 'total_revenue',
                   'revenue_percentage']
        return pd.DataFrame(result, columns=columns)

    @staticmethod
    def _read_and_process_csv(file_path: str, dtypes: dict, date_columns: list, chunk_size: int) -> Iterator[
        pd.DataFrame]:
        for chunk in pd.read_csv(file_path, dtype=dtypes, chunksize=chunk_size):
            for date_col in date_columns:
                # Convert to date 'YYYY-MM-DD'
                if date_columns:
                    chunk[date_col] = pd.to_datetime(chunk[date_col], format='%Y-%m-%d').dt.date
            yield chunk

    def extract_applications(self, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        dtypes = {
            'application_id': 'int',
            'application_name': 'str',
            'is_elegible': 'int'
        }
        date_columns = ['creation_date', 'record_updated_at']

        return self._read_and_process_csv(self._applications_file_path, dtypes, date_columns, chunk_size)

    def extract_mediation_raw_data(self, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        dtypes = {
            'publisher_id': 'str',
            'application_id': 'int',
            'country': 'str',
            'impressions': 'int',
            'clicks': 'int',
            'revenue': 'float'
        }
        date_columns = ['event_date']
        return self._read_and_process_csv(self._mediation_raw_file_path, dtypes, date_columns, chunk_size)

