from unittest.mock import MagicMock, patch, mock_open

import pandas as pd
import pytest

from exercise_1.database.database_manager import DatabaseManager


class TestDatabaseManager:

    @pytest.fixture(autouse=True)
    @patch('exercise_1.database.database_manager.get_file_path')
    @patch('builtins.open', new_callable=mock_open,
           read_data="-- AGGREGATE_DATA\nCREATE TABLE mediation_aggregation;")
    def setup(self, mock_open_file, mock_get_file_path):
        mock_get_file_path.return_value = 'dummy_path.sql'
        self.db_manager = DatabaseManager('test_mediation_data.db', 'dummy_path.sql')
        self.db_manager.conn = MagicMock()
        self.db_manager.cursor = self.db_manager.conn.cursor()

    @patch('pandas.DataFrame.to_sql')
    def test_insert_dataframe(self, mock_to_sql):
        # Given
        df = pd.DataFrame({
            'application_id': [12345],
            'application_name': ['GamesIL'],
            'is_elegible': [1]
        })

        # When
        self.db_manager.insert_dataframe('applications', df)

        # Then
        mock_to_sql.assert_called_once_with('applications', self.db_manager.conn, if_exists='replace', index=False)

    @patch('exercise_1.database.database_manager.get_file_path')
    def test_write_to_csv(self, mock_get_file_path):
        # Given
        mock_get_file_path.return_value = 'test.csv'

        # When
        df = pd.DataFrame({
            'application_id': [12345],
            'application_name': ['GamesIL'],
            'is_elegible': [1]
        })

        # Then
        with patch('pandas.DataFrame.to_csv') as mock_to_csv:
            self.db_manager.write_to_csv(df, 'test.csv')
            mock_to_csv.assert_called_once_with('test.csv', index=False)
