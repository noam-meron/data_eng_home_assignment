# Mediation Data Processor

This project is designed to process mediation data using SQLite and pandas. It involves reading data from CSV files, inserting them into a local SQLite database, performing aggregations, and writing the results to CSV.

## Features

- Reads mediation data and application data from CSV files.
- Inserts the data into a SQLite database.
- Performs aggregations based on the data.
- Writes the aggregated data into a new CSV file: 'exercise_1/data/processed/mediation_aggregation.csv'.

## Installation

### Prerequisites

Ensure you have Python 3.12 installed.

### Install Dependencies

1. Clone this repository:

   ```bash
   git clone git@github.com:noam-meron/data_eng_home_assignment.git
    ```
2. Create virtual environment:

   ```bash
   python -m venv venv
   ```
3. Activate the virtual environment:

   ```bash
   source venv/bin/activate
   ```
   
4. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```
3. Run the main script:

   ```bash
   python main.py
   ```
