import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from urllib.parse import urljoin
from tqdm import tqdm
import s2sphere
import logging
from datetime import datetime
from UA_sw_mappings.UA_sw_mapping import PARAMETER_SELECTION, DATA_ALIASES

# URL of the website containing the dataset
url = 'https://data.gov.ua/dataset/surface-water-monitoring'

# Setup logging configuration
logging.basicConfig(level=logging.INFO)


# Function to download a file from a given URL and return the content
def download_file_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Ensure we notice bad responses
        return response.content
    except requests.RequestException as e:
        logging.error(f"Failed to download file from {url}: {e}")
        return None


# Function to clean CSV content
def load_and_clean_data(csv_content):
    try:
        df = pd.read_csv(StringIO(csv_content.decode('UTF-8-SIG')), delimiter=';', encoding='UTF-8-SIG',
                         on_bad_lines='skip')

        # Drop unnecessary columns safely
        columns_to_drop = [1, 2, 3, 4]
        if len(df.columns) > max(columns_to_drop):
            df.drop(df.columns[columns_to_drop], axis=1, inplace=True)

        # Convert 'Controle_Date' to datetime format (yyyy-mm-dd)
        if 'Controle_Date' in df.columns:
            df['Controle_Date'] = pd.to_datetime(df['Controle_Date'], format='%Y-%m-%d', errors='coerce')
            df.dropna(subset=['Controle_Date'], inplace=True)  # Drop rows with invalid dates

        # Drop any unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        return df
    except Exception as e:
        logging.error(f"Failed to read or clean CSV content: {e}")
        return None


# Function to scrape the website, download CSV files, clean them, and return a combined DataFrame
def scrape_and_combine_data(base_url):
    try:
        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all links to CSV files with class "resource-url-analytics"
        csv_links = [urljoin(base_url, link.get('href'))
                     for link in soup.find_all('a', class_='resource-url-analytics')
                     if link.get('href') and link.get('href').endswith('.csv') and 'output' in link.get('href')]

        # Initialize an empty list to store DataFrames
        dfs = []
        for csv_url in tqdm(csv_links, desc="Processing CSV files"):
            csv_content = download_file_content(csv_url)
            if csv_content:
                cleaned_df = load_and_clean_data(csv_content)
                if cleaned_df is not None:
                    dfs.append(cleaned_df)

        dfs = [df for df in dfs if not df.empty]  # Filter out any empty DataFrames
        combined_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        # Add custom headers if the number of columns matches
        if len(combined_df.columns) == len(PARAMETER_SELECTION):
            combined_df.columns = PARAMETER_SELECTION
        else:
            logging.warning("Number of columns does not match the number of custom headers.")

        # Sort the combined DataFrame by 'id' and 'date'
        if 'id' in combined_df.columns and 'date' in combined_df.columns:
            combined_df = combined_df.sort_values(by=['id', 'date'], ascending=True)

        # Add S2 cell ID based on coordinates if latitude and longitude are available
        if "lat" in combined_df.columns and "lon" in combined_df.columns:
            combined_df['S2CELL'] = combined_df.apply(lambda x: s2sphere.CellId.from_lat_lng(
                s2sphere.LatLng.from_degrees(x['lat'], x['lon'])).parent(18).id() if pd.notnull(
                x['lat']) and pd.notnull(x['lon']) else None, axis=1)

        return combined_df
    except Exception as e:
        logging.error(f"Error during data scraping and combination: {e}")
        return pd.DataFrame()


def _limit_coordinates(spatial_range, coordinates):
    n, s, e, w = spatial_range
    return coordinates[(coordinates.lat <= n) & (coordinates.lat >= s) &
                       (coordinates.lon <= e) & (coordinates.lon >= w)]


def _prepare_coordinates(spatial_range, level):
    try:
        coordinates = pd.read_csv("constants/UA_coordinates.csv")
        coordinates.lat = coordinates.lat.astype('float32')
        coordinates.lon = coordinates.lon.astype('float32')
        coordinates = _limit_coordinates(spatial_range=spatial_range, coordinates=coordinates)
        coordinates['S2CELL'] = coordinates.apply(lambda x:
                                                  s2sphere.CellId.from_lat_lng(
                                                      s2sphere.LatLng.from_degrees(x.lat, x.lon)).parent(level).id(),
                                                  axis=1)
        return coordinates
    except Exception as e:
        logging.error(f"Error preparing coordinates: {e}")
        return pd.DataFrame()


def read_data(spatial_range, time_range, data_range, level):
    time_from, time_to = time_range
    try:
        time_from = datetime.strptime(time_from, '%Y-%m-%d').year
        time_to = datetime.strptime(time_to, '%Y-%m-%d').year
        coordinates = _prepare_coordinates(spatial_range=spatial_range, level=level)

        final_df = scrape_and_combine_data(url)
        if final_df.empty:
            logging.warning("No data to process in `read_data`.")
            return pd.DataFrame()

        # Filter by time range
        if 'date' in final_df.columns:
            final_df['date'] = pd.to_datetime(final_df['date'], errors='coerce')
            final_df = final_df[(final_df['date'].dt.year >= time_from) & (final_df['date'].dt.year <= time_to)]

        # Map data range to available parameters in final_df
        data_requested = {k for k, v in DATA_ALIASES.items() if v in data_range}
        parameter_selection = list(set(PARAMETER_SELECTION).intersection(data_requested))

        # Filter columns based on parameter selection
        columns_to_select = ['S2CELL', 'date'] + parameter_selection
        columns_to_select = [col for col in columns_to_select if col in final_df.columns]
        final_df = final_df[columns_to_select]
        return final_df

    except Exception as e:
        logging.error(f"Error reading data: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    final_data = scrape_and_combine_data(url)
    N, S, E, W = 59.0, 49.0, 24.2, 15.2
    LEVEL = 18
    TIME_FROM, TIME_TO = '2003-01-02', '2024-03-31'
    FACTORS = ["sw quality"]

    result = read_data(spatial_range=(N, S, E, W), time_range=(TIME_FROM, TIME_TO),
                       data_range=FACTORS, level=LEVEL)
    print(result)
