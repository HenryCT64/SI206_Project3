import requests
import sqlite3
import os
from bs4 import BeautifulSoup
import pandas as pd
import random
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

CENSUS_API_KEY = "a04e80b745e5ab02d0d22161312ca4b7fa0cf548"
CENSUS_API_URL = "https://api.census.gov/data/2020/acs/acs5"

YELP_API_KEY = "KjF7UADaggGSq9SzU4iXosZO9sYx-taIgJMiDMyA1lGWo8fcs0aCjkSIu2RmCrN_52mpOXbq5vK3zzdFpW1Cjn7rI-N-ICNP9TCUekqLXugR0aZCKZLXJb6dNY5gZ3Yx"
YELP_API_URL = "https://api.yelp.com/v3/businesses/search"

DATABASE_NAME = "project_data.db"

def setup_database():

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS IncomeData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zip_code TEXT NOT NULL UNIQUE,
            median_income INTEGER,
            population INTEGER );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS BusinessCategory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL UNIQUE );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS YelpData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_name TEXT,
            zip_code TEXT NOT NULL,
            rating REAL,
            num_reviews INTEGER,
            category_id INTEGER NOT NULL,
            FOREIGN KEY (category_id) REFERENCES BusinessCategory(id) );
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

def save_census_data_to_database(data):
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT OR IGNORE INTO IncomeData (zip_code, median_income, population)
        VALUES (?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

def fetch_yelp_data(zip_code):

    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {
        "location": zip_code,
        "limit": 5 # Only grab 5 businesses per zip each time file is run  
    }

    response = requests.get(YELP_API_URL, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    processed_data = []

    for business in data.get("businesses", []):

        try:

            business_name = business["name"]
            rating = business["rating"]
            num_reviews = business["review_count"]
            category = business["categories"][0]["title"] if business["categories"] else None
            processed_data.append((business_name, zip_code, rating, num_reviews, category))

        except KeyError:
            continue
    
    return processed_data

def get_or_create_category_id(category):
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO BusinessCategory (category) VALUES (?)
    """, (category,))
    conn.commit()

    # Fetch the id of the category
    cursor.execute("""
        SELECT id FROM BusinessCategory WHERE category = ?
    """, (category,))
    category_id = cursor.fetchone()[0]

    conn.close()
    return category_id

def save_yelp_data_to_database(data):
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    normalized_data = []
    for business_name, zip_code, rating, num_reviews, category in data:
        if not category:
            continue  # Skip if the category is missing

        # Get or create the category ID
        category_id = get_or_create_category_id(category)

        # Prepare data for insertion
        normalized_data.append((business_name, zip_code, rating, num_reviews, category_id))

    # Insert data into YelpData
    cursor.executemany("""
        INSERT INTO YelpData (business_name, zip_code, rating, num_reviews, category_id)
        VALUES (?, ?, ?, ?, ?)
    """, normalized_data)

    conn.commit()
    conn.close()

def fetch_and_store_yelp_data():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    # Get all distinct ZIP codes from IncomeData
    cursor.execute("SELECT zip_code FROM IncomeData")
    zip_codes = [row[0] for row in cursor.fetchall()]
    random.shuffle(zip_codes)
    conn.close()

    # only check 5 zip codes
    zip_codes = zip_codes[:5]
    
    for zip_code in zip_codes:
        yelp_data = fetch_yelp_data(zip_code)
        save_yelp_data_to_database(yelp_data)

def ave_yelp_ratings():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT zip_code, AVG(rating) as avg_rating 
    FROM YelpData 
    GROUP BY zip_code
    """)

    result = cursor.fetchall()
    conn.close()

    return result

def get_income_data():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("SELECT zip_code, median_income FROM IncomeData")
    result = cursor.fetchall()
    conn.close()

    return result

def create_scatter_plot1(avg_ratings, income_data):
    df_avg_ratings = pd.DataFrame(avg_ratings, columns=['zip_code', 'avg_rating'])
    df_income_data = pd.DataFrame(income_data, columns=['zip_code', 'median_income'])

    merged_df = pd.merge(df_avg_ratings, df_income_data, on='zip_code')

    plt.figure(figsize=(10, 6))
    plt.scatter(merged_df['median_income'], merged_df['avg_rating'], marker = "*", color = "red")
    plt.title('Relationship between Median Household Income and Average Yelp Ratings by ZIP Code')
    plt.xlabel('Median Household Income')
    plt.ylabel('Average Yelp Rating')
    plt.ylim(3,5)
    plt.grid(True)
    plt.savefig('Income_Ratings_Scatterplot.png')

def business_count():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT COUNT(yd.id) as business_count
    FROM YelpData yd
    JOIN BusinessCategory bc ON yd.category_id = bc.id
    GROUP BY bc.category
    """)

    result = cursor.fetchall()
    conn.close()
    return [row[0] for row in result]

def create_histogram(business_data):
    plt.figure(figsize=(12, 8))
    plt.hist(business_data, bins=range(min(business_data), max(business_data) + 1), color='blue')
    plt.title('Histogram of Total Number of Businesses per Category')
    plt.xlabel('Number of Businesses')
    plt.ylabel('Frequency of Categories')
    plt.savefig('Business_Category_Histogram.png')

def main():

    setup_database()
    
    # Obtain Census Data
    print("Fetching up to 25 distinct ZIP Codes from the Census data")
    census_data = fetch_census_data()
    print(f"Saving {len(census_data)} new records to the database")
    save_census_data_to_database(census_data)
    
    # Fetch and Store Yelp Data
    print("Fetching Yelp Data...")
    fetch_and_store_yelp_data()

    print("Creating scatterplot...")
    create_scatter_plot1(ave_yelp_ratings(), get_income_data())

    print("Creating histogram...")
    create_histogram(business_count())
    

if __name__ == "__main__":
    main()