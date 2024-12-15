import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# Constants for API keys and URLs
API_KEY = "a04e80b745e5ab02d0d22161312ca4b7fa0cf548"
US_CENSUS_URL = "https://api.census.gov/data/{year}/acs/acs5"
INT_CENSUS_URL = "https://api.census.gov/data/timeseries/idb/5year"

# Remove footnotes from text
def remove_footnotes(text):
    return text.split('[')[0].strip()

def get_us_population_data(year):
    params = {
        "get": "NAME,B01003_001E",
        "for": "state:*",
        "key": API_KEY
    }
    response = requests.get(US_CENSUS_URL.format(year=year), params=params)
    
    data = response.json()
    columns = data[0]
    rows = data[1:]
    df = pd.DataFrame(data=rows, columns=columns)

    df.rename(columns={
        "NAME": "State",
        "B01003_001E": f"Population_{year}"
    }, inplace=True)

    df[f"Population_{year}"] = pd.to_numeric(df[f"Population_{year}"], errors='coerce')
    if 'state' in df.columns:
        df.drop(columns=['state'], inplace=True)

    return df


def get_int_census_data(year):
    params = {
        "get": "NAME,POP",
        "time": year,
        "key": API_KEY
    }
    response = requests.get(INT_CENSUS_URL, params=params)
    data = response.json()
    columns = data[0]
    rows = data[1:]
    df = pd.DataFrame(data=rows, columns=columns)

    df.rename(columns={
        "NAME": "Country",
        "POP": f"Population_{year}"
    }, inplace=True)

    df = df[~df['Country'].str.contains(';')]
    df['Country'] = df['Country'].apply(remove_footnotes)

    df[f"Population_{year}"] = pd.to_numeric(df[f"Population_{year}"], errors='coerce')
    if 'time' in df.columns:
        df.drop(columns=['time'], inplace=True)
        
    return df


def get_us_wikipedia_data():
    url = "https://en.wikipedia.org/wiki/List_of_U.S._states_and_territories_by_population"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')

    data = []
    for row in rows[1:]:
        cols = row.find_all(['th', 'td'])
        state = remove_footnotes(cols[0].text.strip())
        population_2023 = cols[1].text.strip().replace(',', '')
        population_2020 = cols[2].text.strip().replace(',', '')

        data.append([state, population_2023, population_2020])

    df = pd.DataFrame(data, columns=['State', 'Population_2023', 'Population_2020'])
    df['Population_2023'] = pd.to_numeric(df['Population_2023'], errors='coerce')
    df['Population_2020'] = pd.to_numeric(df['Population_2020'], errors='coerce')
    return df


def get_world_wikipedia_data():
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')

    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        country = remove_footnotes(cols[0].text.strip())
        population = cols[1].text.strip().replace(',', '')

        data.append([country, population])

    df = pd.DataFrame(data, columns=['Country', 'Wikipedia_Population'])
    df['Wikipedia_Population'] = pd.to_numeric(df['Wikipedia_Population'], errors='coerce')
    return df


def get_world_chinese_wikipedia_data():
    url = "https://zh.wikipedia.org/wiki/%E4%B8%96%E7%95%8C%E5%9B%BD%E5%AE%B6%E5%92%8C%E5%9C%B0%E5%8C%BA%E4%BA%BA%E5%8F%A3%E6%8E%92%E5%90%8D%E5%88%97%E8%A1%A8"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')

    data = []
    for row in rows[1:]:
        cols = row.find_all('td')
        country = remove_footnotes(cols[1].text.strip())
        pop2017 = cols[5].text.strip().replace(',', '')
        data.append([country, pop2017])

    df = pd.DataFrame(data, columns=['Country', 'Population_2017'])
    df['Population_2017'] = pd.to_numeric(df['Population_2017'], errors='coerce')
    return df


def save_to_database(df, table_name):
    engine = create_engine('sqlite:///census_data.db')
    with engine.connect() as conn:
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)


def main():
    df_2020 = get_us_population_data(2020)
    df_2023 = get_us_population_data(2023)
    us_combined_df = df_2023.merge(df_2020, on="State")
    save_to_database(us_combined_df, 'us_population_data')
    print("U.S. Census data for 2020 and 2023 saved")
    
    save_to_database(get_int_census_data("2017"), 'int_census_data')
    print("International Census data saved")

    save_to_database(get_us_wikipedia_data(), 'us_wikipedia_data')
    print("US State Wikipedia data saved")

    save_to_database(get_world_wikipedia_data(), 'world_wikipedia_data')
    print("World Wikipedia data saved")

    save_to_database(get_world_chinese_wikipedia_data(), 'world_chinese_wikipedia_data')
    print("Chinese Wikipedia data saved")

if __name__ == "__main__":
    main()