import requests
import pandas as pd
from sqlalchemy import create_engine

API_KEY = "a04e80b745e5ab02d0d22161312ca4b7fa0cf548"
BASE_URL = "https://api.census.gov/data/timeseries/idb/5year"

def get_population_data():
    # Example: Getting population data for all available countries
    params = {
        "get": "NAME,POP",
        "time": "2020",  # Replace with the desired year or 'recent'
        "key": API_KEY
    }
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        columns = data[0]
        rows = data[1:]
        df = pd.DataFrame(data=rows, columns=columns)

        # Rename and clean up the columns if necessary
        df.rename(columns={
            "NAME": "Country",
            "POP": "Population"
        }, inplace=True)

        # Convert numeric columns to appropriate types
        df["Population"] = pd.to_numeric(df["Population"], errors='coerce')

        return df
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

def save_to_database(df):
    # Create a SQLite database connection
    engine = create_engine('sqlite:///world_population_data.db')
    conn = engine.connect()

    # Save the data to the database. Replace 'world_population_data' with your table name
    df.to_sql('world_population_data', con=conn, if_exists='replace', index=False)
    conn.close()

def main():
    # Step 1: Get population data
    df = get_population_data()
    if df is not None:
        print("Data collected successfully.")
        print(df.head())

        # Step 2: Save data to database
        save_to_database(df)
        print("Data saved to the database successfully.")
    else:
        print("Failed to retrieve data.")

if __name__ == "__main__":
    main()