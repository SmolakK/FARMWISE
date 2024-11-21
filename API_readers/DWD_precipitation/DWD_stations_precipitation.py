import csv


def process_txt_to_csv(input_file, output_file):
    # Try reading the file using a different encoding (ISO-8859-1)
    with open(input_file, 'r', encoding='ISO-8859-1') as txt_file:
        lines = txt_file.readlines()

    # Open CSV file for writing
    with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)

        # Write new headers to the CSV
        writer.writerow(['id', 'name', 'lat', 'lon'])

        # Process each line after the header
        for line in lines[1:]:  # Skip the header line
            columns = line.split()

            # Skip rows with insufficient columns or with placeholders like '--------'
            if len(columns) < 8 or '--------' in line:
                continue

            station_id = columns[0].strip()  # ST_ID
            name = columns[6].strip()  # Stationsname
            lat = columns[4].strip()  # geo_Breite (latitude)
            lon = columns[5].strip()  # geo_Laenge (longitude)

            # Skip rows if any essential field is missing (e.g., lat, lon, or name)
            if not station_id or not name or not lat or not lon:
                continue

            # Write the processed data to the CSV
            writer.writerow([station_id, name, lat, lon])


# Call the function with the input txt file and desired output CSV file name

process_txt_to_csv('constants/RR_Tageswerte_Beschreibung_Stationen.txt', 'constants/DWD_stations_precipitation.csv')
