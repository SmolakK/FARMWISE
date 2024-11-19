import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from urllib.parse import urljoin
from tqdm import tqdm

url = 'https://data.gov.ua/dataset/surface-water-monitoring'
selected_columns = ['Post_ID', 'Post_Name', 'Latitude', 'Longitude']
columns = {
    'Post_ID': 'id',
    'Post_Name': 'name',
    'Latitude': 'lat',
    'Longitude': 'lon'
}


def fetch_csv_links(base_url):
    """Fetch all CSV file links from the base URL."""
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return [urljoin(base_url, link.get('href')) for link in soup.find_all('a', class_='resource-url-analytics')
            if link.get('href') and link.get('href').endswith('.csv') and 'output' in link.get('href')]


def load_selected_data(csv_url):
    """Download and load CSV data, selecting only specified columns if available, and rename columns."""
    response = requests.get(csv_url)
    response.raise_for_status()
    df = pd.read_csv(StringIO(response.content.decode('UTF-8-SIG')), delimiter=';', encoding='UTF-8-SIG',
                     usecols=lambda col: col in selected_columns, on_bad_lines='skip')

    # Drop rows with missing selected columns and rename columns
    df = df.dropna(subset=selected_columns, how='any')
    df.rename(columns=columns, inplace=True)

    # Convert Latitude and Longitude to float
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    return df


def scrape_and_process_data(base_url):
    """Scrape CSV files, load selected columns, rename headers, and process unique coordinates."""
    csv_links = fetch_csv_links(base_url)
    dfs = [load_selected_data(csv_url) for csv_url in tqdm(csv_links, desc="Processing CSV files")]

    # Combine all DataFrames, drop duplicate coordinates, and sort by new Latitude and Longitude names
    combined_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['lat', 'lon'])
    combined_df.sort_values(by=['lat', 'lon'], inplace=True)

    # Save to CSV with updated headers
    combined_df.to_csv('UA_coordinates.csv', index=False)
    print("Data has been processed and saved to 'UA_coordinates.csv'.")
    return combined_df


# Execute function
scrape_and_process_data(url)
