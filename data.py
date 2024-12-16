import requests
import sqlite3
import os
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

CENSUS_API_KEY = "a04e80b745e5ab02d0d22161312ca4b7fa0cf548"
CENSUS_API_URL = "https://api.census.gov/data/2020/acs/acs5"

DATABASE_NAME = "project_data.db"

def setup_database():

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Create Income Data table
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS IncomeData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zip_code TEXT NOT NULL,
            median_income INTEGER,
            population INTEGER );
        """
    )

    conn.commit()
    conn.close()

def get_existing_zip_codes():
    """
    Fetches the ZIP codes already stored in the database to ensure distinct entries.
    
    Returns:
        set: A set of ZIP codes that are already stored in the database.
    """
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT zip_code FROM IncomeData")
    existing_zip_codes = {row[0] for row in cursor.fetchall()}
    conn.close()
    return existing_zip_codes

def fetch_census_data():

    params = {
        "get": "B19013_001E,NAME,B01003_001E",
        "for": "zip code tabulation area:*",
        "key": CENSUS_API_KEY
    }
    
    response = requests.get(CENSUS_API_URL, params=params)

    # Raise an error if the request fails
    response.raise_for_status()
    
    data = response.json()
    headers, rows = data[0], data[1:]

    # Make sure we get 25 distinct zip codes
    existing_zip_codes = get_existing_zip_codes()
    
    # Extract relevant data
    processed_data = []
    for row in rows:

        try:

            zip_code = row[3]

            if zip_code in existing_zip_codes:
                continue

            median_income = row[0]
            if median_income == 'null' or int(median_income) < 0:
                continue
            median_income = int(median_income)

            population = row[2]
            if population == 'null' or int(population) <= 0:
                continue
            population = int(population)

            processed_data.append((zip_code, median_income, population))

            # Limit to 25 items per execution
            if len(processed_data) >= 25:
                break

        except (ValueError, IndexError):
            continue
    
    return processed_data


def save_to_database(data):
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT OR IGNORE INTO IncomeData (zip_code, median_income, population)
        VALUES (?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

def main():

    setup_database()

    print("Fetching up top 25 distinct ZIP Codes from the Census data")
    census_data = fetch_census_data()

    save_to_database(census_data)
    print(f"Saved {len(census_data)} new records to the database")

if __name__ == "__main__":
    main()