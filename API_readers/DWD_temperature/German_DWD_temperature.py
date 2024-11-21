import requests
from bs4 import BeautifulSoup
import zipfile
import io
import s2sphere
import pandas as pd
import logging
from tqdm import tqdm
from datetime import datetime
from DWD_temperature_mapping.DWD_mapping import DATA_ALIASES, PARAMETER_SELECTION

base_url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical/"
desired_headers = list(DATA_ALIASES.keys())

# station_numbers = ["00044", "00056"] #TEST,to be changed later for list from csv (code below)

DWD_stations_temperature = pd.read_csv("constants/DWD_stations_temperature.csv")
station_numbers = DWD_stations_temperature.iloc[:, 0].astype(str).str.zfill(5).tolist()


def get_data():
    try:
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching base URL: {e}")
        return pd.DataFrame()

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
                zip_links.append(base_url + href)

    all_dataframes = []
    for zip_url in tqdm(zip_links, desc="Processing .zip files"):
        try:
            response = requests.get(zip_url)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for file_name in z.namelist():
                    if file_name.startswith("produkt_tu_stunde_"):
                        with z.open(file_name) as file:
                            df = pd.read_csv(file, sep=";", skiprows=1).iloc[:, [0, 1, 3]]
                            df.columns = desired_headers[:3]  # id, date, Air temperature
                            df["date"] = pd.to_datetime(df["date"], format='%Y%m%d%H')
                            df["id"] = df["id"].astype(str)  # Convert to string for merging
                            df = df[(df != -999).all(axis=1)]
                            all_dataframes.append(df)
        except Exception as e:
            print(f"Error processing {zip_url}: {e}")

    final_df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame(columns=desired_headers[:3])
    final_df = final_df[(final_df['date'] >= TIME_FROM) & (final_df['date'] <= TIME_TO)]

    # Merge latitude and longitude information for S2CELL calculation
    station_coordinates = pd.read_csv('constants/DWD_stations_temperature.csv')
    station_coordinates['id'] = station_coordinates['id'].astype(str)  # Ensure 'id' is string for merging
    final_df = final_df.merge(station_coordinates, on="id", how="left")  # Merge station coordinates

    # Add S2CELL IDs if latitude and longitude are available
    if "lat" in final_df.columns and "lon" in final_df.columns:
        final_df['S2CELL'] = final_df.apply(
            lambda x: s2sphere.CellId.from_lat_lng(
                s2sphere.LatLng.from_degrees(x['lat'], x['lon'])
            ).parent(18).id() if pd.notnull(x['lat']) and pd.notnull(x['lon']) else None, axis=1
        )
        # Drop lat and lon after S2CELL calculation
        final_df.drop(columns=["lat", "lon"], inplace=True)

    return final_df


def _limit_coordinates(spatial_range, coordinates):
    n, s, e, w = spatial_range
    coordinates = coordinates[(coordinates.lat <= n) & (coordinates.lat >= s) &
                              (coordinates.lon <= e) & (coordinates.lon >= w)]
    return coordinates


def _prepare_coordinates(spatial_range, level):
    coordinates = pd.read_csv('constants/DWD_stations_temperature.csv')
    coordinates.lat = coordinates.lat.astype('float32')
    coordinates.lon = coordinates.lon.astype('float32')
    coordinates = _limit_coordinates(spatial_range=spatial_range, coordinates=coordinates)
    coordinates['S2CELL'] = coordinates.apply(
        lambda x: s2sphere.CellId.from_lat_lng(
            s2sphere.LatLng.from_degrees(x.lat, x.lon)).parent(level).id(),
        axis=1
    )
    return coordinates


def read_data(spatial_range, time_range, data_range, level):
    # Extract years from time range
    time_from, time_to = time_range
    time_from = datetime.strptime(time_from, '%Y-%m-%d').year
    time_to = datetime.strptime(time_to, '%Y-%m-%d').year
    coordinates = _prepare_coordinates(spatial_range=spatial_range, level=level)

    final_df = get_data()
    if final_df.empty:
        logging.warning("No data to process in `read_data`.")
        return pd.DataFrame()

    final_df = final_df[(final_df['date'] >= datetime(time_from, 1, 1)) & (final_df['date'] <= datetime(time_to, 12, 31))]

    data_requested = set([k for k, v in DATA_ALIASES.items() if v in data_range])
    parameter_selection = list(set(PARAMETER_SELECTION).intersection(data_requested))

    columns_to_select = ['S2CELL', 'date'] + parameter_selection
    columns_to_select = [col for col in columns_to_select if col in final_df.columns]
    final_df = final_df[columns_to_select]

    return final_df


if __name__ == "__main__":
    N, S, E, W = 55.0557, 47.3025, 15.0419, 5.8663
    LEVEL = 18
    TIME_FROM, TIME_TO = '2023-01-01', '2023-12-31'
    FACTORS = ["temperature"]

    result = read_data(spatial_range=(N, S, E, W), time_range=(TIME_FROM, TIME_TO),
                       data_range=FACTORS, level=LEVEL)
    print(result)
