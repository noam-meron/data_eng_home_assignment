import logging

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data_to_db
from etl.utils import get_db_engine


def run_etl_pipeline():
    """
    Orchestrate the ETL pipeline: Extract -> Transform -> Load.
    """
    url = 'https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv'
    table_name = 'iris_data'
    engine = get_db_engine()

    logging.info("Starting data extraction")
    for chunk in extract_data(url):
        logging.info(f"Extracted {len(chunk)} rows")

        logging.info("Starting data transformation")
        transformed_chunk = transform_data(chunk)

        logging.info("Loading data to the database")
        load_data_to_db(transformed_chunk, table_name, engine)

    logging.info("ETL pipeline completed successfully.")


if __name__ == "__main__":
    run_etl_pipeline()
