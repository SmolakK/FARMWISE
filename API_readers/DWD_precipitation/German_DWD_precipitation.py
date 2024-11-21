import requests
from bs4 import BeautifulSoup
import zipfile
import io
import s2sphere
import pandas as pd
import logging
from tqdm import tqdm
from datetime import datetime
from DWD_precipitation_mapping.DWD_mapping import DATA_ALIASES, PARAMETER_SELECTION


url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/more_precip/historical/"

desired_headers = list(DATA_ALIASES.keys())

# station_numbers = ["00002", "00010"] #TEST, to be changed later for list from csv

DWD_stations_precipitation = pd.read_csv("constants/DWD_stations_precipitation.csv")
station_numbers = DWD_stations_precipitation.iloc[:, 0].astype(str).str.zfill(5).tolist()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_data():
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Error fetching base URL: {e}")
        return pd.DataFrame()  # Return empty DataFrame if there's an error

    soup = BeautifulSoup(response.text, 'html.parser')
    zip_links = []
    for a_tag in soup.find_all('a'):
        href = a_tag.get('href')
        if href.endswith('.zip'):
            filename = href.split('/')[-1]
            station_number = filename.split('_')[2]
            date_part = filename.split('_')[3:5]
            file_start_date = datetime.strptime(date_part[0], '%Y%m%d')
            file_end_date = datetime.strptime(date_part[1], '%Y%m%d')

            if file_end_date >= pd.to_datetime(TIME_FROM) and file_start_date <= pd.to_datetime(TIME_TO) and station_number in station_numbers:
                zip_links.append(url + href)

    all_dataframes = []
    for zip_url in tqdm(zip_links, desc="Processing .zip files"):
        try:
            response = requests.get(zip_url)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_name in z.namelist():
                    if file_name.startswith("produkt_nieder_tag_"):
                        with z.open(file_name) as file:
                            df = pd.read_csv(file, sep=";", skiprows=1).iloc[:, [0, 1, 3]]
                            df.columns = desired_headers[:3]  # Only id, date, precipitation
                            df["date"] = pd.to_datetime(df["date"], format='%Y%m%d')
                            df['year'] = df['date'].dt.year  # Add year for filtering
                            df['id'] = df['id'].astype(str)  # Convert to string for merging
                            all_dataframes.append(df)
        except Exception as e:
            logging.error(f"Error processing {zip_url}: {e}")

    final_df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame(columns=desired_headers[:3])
    final_df = final_df[(final_df['date'] >= TIME_FROM) & (final_df['date'] <= TIME_TO)]

    # Merge latitude and longitude information for S2CELL calculation
    station_coordinates = pd.read_csv('constants/DWD_stations_precipitation.csv')
    station_coordinates['id'] = station_coordinates['id'].astype(str)  # Convert to string for merging
    final_df = final_df.merge(station_coordinates, on="id", how="left")  # Merge station coordinates

    # Add S2CELL IDs if latitude and longitude are available
    if "lat" in final_df.columns and "lon" in final_df.columns:
        final_df['S2CELL'] = final_df.apply(
            lambda x: s2sphere.CellId.from_lat_lng(
                s2sphere.LatLng.from_degrees(x['lat'], x['lon'])
            ).parent(18).id() if pd.notnull(x['lat']) and pd.notnull(x['lon']) else None, axis=1
        )

    return final_df


def _limit_coordinates(spatial_range, coordinates):
    """
    Limit the coordinates DataFrame to those falling within the specified spatial range.

    :param spatial_range: A tuple containing the spatial range (N, S, E, W) defining the bounding box.
    :param coordinates: DataFrame containing latitude and longitude coordinates.
    :return: DataFrame containing coordinates within the specified spatial range.
    """
    n, s, e, w = spatial_range
    coordinates = coordinates[(coordinates.lat <= n) & (coordinates.lat >= s) &
                              (coordinates.lon <= e) & (coordinates.lon >= w)]
    return coordinates


def _prepare_coordinates(spatial_range, level):
    """
    Prepare coordinates for data retrieval, limiting them to the specified spatial range and assigning S2Cell IDs.

    :param spatial_range: A tuple containing the spatial range (N, S, E, W) defining the bounding box.
    :param level: S2Cell level.
    :return: DataFrame containing coordinates within the specified spatial range and their corresponding S2Cell IDs.
    """
    coordinates = pd.read_csv('constants/DWD_stations_precipitation.csv')
    coordinates.lat = coordinates.lat.astype('float32')
    coordinates.lon = coordinates.lon.astype('float32')
    coordinates = _limit_coordinates(spatial_range=spatial_range, coordinates=coordinates)
    coordinates['S2CELL'] = coordinates.apply(
        lambda x: s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(x.lat, x.lon)).parent(level).id(),
        axis=1
    )
    return coordinates


def read_data(spatial_range: object, time_range: object, data_range: object, level: object) -> object:
    # Extract years from time range
    time_from, time_to = time_range
    time_from = datetime.strptime(time_from, '%Y-%m-%d').year
    time_to = datetime.strptime(time_to, '%Y-%m-%d').year
    coordinates = _prepare_coordinates(spatial_range=spatial_range, level=level)

    # Get the final data from the `get_data` function
    final_df = get_data()
    if final_df.empty:
        logging.warning("No data to process in `read_data`.")
        return pd.DataFrame()  # Return an empty DataFrame if no data

    # Filter `final_df` by time range
    final_df = final_df[(final_df['year'] >= time_from) & (final_df['year'] <= time_to)]

    # Map `data_range` to available parameters in `final_df`
    data_requested = set([k for k, v in DATA_ALIASES.items() if v in data_range])
    parameter_selection = list(set(PARAMETER_SELECTION).intersection(data_requested))

    # Filter columns in `final_df` based on `parameter_selection` and add `year`, `id`, and `S2CELL` if present
    columns_to_select = ['S2CELL', 'date'] + parameter_selection
    columns_to_select = [col for col in columns_to_select if col in final_df.columns]  # Ensure columns exist
    final_df = final_df[columns_to_select]

    return final_df


if __name__ == "__main__":
    N, S, E, W = 55.0557, 47.3025, 15.0419, 5.8663
    LEVEL = 18
    TIME_FROM, TIME_TO = '2000-01-01', '2023-12-31'
    FACTORS = ["precipitation"]

    result = read_data(spatial_range=(N, S, E, W), time_range=(TIME_FROM, TIME_TO),
                       data_range=FACTORS, level=LEVEL)
    print(result)
