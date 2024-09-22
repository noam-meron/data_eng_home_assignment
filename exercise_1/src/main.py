import logging

from exercise_1.database.data_processor import DataProcessor
from exercise_1.database.database_manager import DatabaseManager


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    db_manager = DatabaseManager('mediation_data.db', 'queries.sql')
    db_manager.connect()
    processor = DataProcessor(db_manager)

    try:
        for df_chunk in processor.extract_applications():
            db_manager.insert_dataframe('applications', df_chunk)

        for df_chunk in processor.extract_mediation_raw_data():
            db_manager.insert_dataframe('mediation_raw_data', df_chunk)

        aggregated_data = processor.aggregate_data()
        db_manager.insert_dataframe('mediation_aggregation', aggregated_data)
        db_manager.write_to_csv(aggregated_data, 'mediation_aggregation.csv')

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
