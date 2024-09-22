import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from exercise_1.database.data_processor import DataProcessor
from exercise_1.database.database_manager import DatabaseManager


sample_applications_data = pd.DataFrame({
    'application_id': [12345, 67893],
    'application_name': ['GamesIL', 'Aooio'],
    'is_elegible': [1, 0],
    'creation_date': ['2022-01-01', '2023-01-01'],
    'record_updated_at': ['2023-07-01', '2023-07-01']
})

sample_mediation_raw_data = pd.DataFrame({
    'publisher_id': ['AAAB', 'BBCC'],
    'application_id': [12345, 67893],
    'country': ['US', 'CA'],
    'impressions': [200, 165],
    'clicks': [34, 12],
    'revenue': [337.5, 143.2],
    'event_date': ['2023-06-01', '2023-06-01']
})


class TestDataProcessor:

    @pytest.fixture(autouse=True)
    def setup(self):
        self.db_manager = MagicMock(DatabaseManager)
        self.data_processor = DataProcessor(self.db_manager)

    @patch('exercise_1.database.data_processor.pd.read_csv')
    def test_extract_applications(self, mock_read_csv):
        # Given
        mock_read_csv.return_value = [sample_applications_data]

        # When
        extracted_data = list(self.data_processor.extract_applications())

        # Then
        assert len(extracted_data) == 1
        assert extracted_data[0].equals(sample_applications_data)
        assert 'creation_date' in extracted_data[0]
        assert 'record_updated_at' in extracted_data[0]

    @patch('exercise_1.database.data_processor.pd.read_csv')
    def test_extract_mediation_raw_data(self, mock_read_csv):
        # Given
        mock_read_csv.return_value = [sample_mediation_raw_data]

        # When
        extracted_data = list(self.data_processor.extract_mediation_raw_data())

        # Then
        assert len(extracted_data) == 1
        assert extracted_data[0].equals(sample_mediation_raw_data)
        assert 'event_date' in extracted_data[0]

    @patch('exercise_1.database.data_processor.pd.read_csv')
    def test_date_conversion_in_extract_applications(self, mock_read_csv):
        # Given
        sample_data_with_dates = sample_applications_data.copy()
        sample_data_with_dates['creation_date'] = pd.to_datetime(sample_data_with_dates['creation_date'], format='%Y-%m-%d').dt.date
        sample_data_with_dates['record_updated_at'] = pd.to_datetime(sample_data_with_dates['record_updated_at'], format='%Y-%m-%d').dt.date
        mock_read_csv.return_value = [sample_applications_data]

        # When
        extracted_data = list(self.data_processor.extract_applications())

        # Then
        assert extracted_data[0]['creation_date'].dtype == 'O'
        assert extracted_data[0]['record_updated_at'].dtype == 'O'
        assert extracted_data[0].equals(sample_data_with_dates)

    @patch('exercise_1.database.data_processor.pd.read_csv')
    def test_date_conversion_in_extract_mediation_raw_data(self, mock_read_csv):
        # Given
        sample_data_with_dates = sample_mediation_raw_data.copy()
        sample_data_with_dates['event_date'] = pd.to_datetime(sample_data_with_dates['event_date'], format='%Y-%m-%d').dt.date
        mock_read_csv.return_value = [sample_mediation_raw_data]

        # When
        extracted_data = list(self.data_processor.extract_mediation_raw_data())

        # Then
        assert extracted_data[0]['event_date'].dtype == 'O'  # Object (date)
        assert extracted_data[0].equals(sample_data_with_dates)
