# %%
import os
import re
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
import time
from dotenv import load_dotenv

load_dotenv()

PATH_ROOT = os.getenv('PATH_ROOT')
DOWNLOAD_PATH = os.getenv('DOWNLOAD_PATH')
DOWNLOAD_FILE_NAME = os.getenv('DOWNLOAD_FILE_NAME')

NUMERIC_COLUMNS = [
    'pris',
    'prisantydning',
    'omkostninger',
    'antall-rom',
    'antall-soverom',
    'internt-bruksareal',
    'bruksareal',
    'byggeår',
    'eksternt-bruksareal',
    'etasje',
    'felleskostnader',
]


# %%


# Some values can be hyphenated, e.g. 1000-1500, especially if the ad is a new building project.
# This function averages the values.
def average_hyphenated_values(value):
    try:
        if isinstance(value, (int, float, np.number)) or '-' not in value:
            return value
        parts = value.split('-')

        numbers = []
        for part in parts:
            numeric_part = re.findall(r'\d+(?:\.\d+)?', part)
            numbers.extend([float(num) for num in numeric_part])
        return round(sum(numbers) / len(numbers)) if numbers else pd.NA
    except Exception as e:
        return pd.NA


# Function to get latitude and longitude from the address using geocoding
def get_lat_long(row, geocode_counter):
    geolocator = Nominatim(user_agent="your_geocoding_app_name")
    # clean up the address string by removing any pairs of parentheses and their contents
    formatted_address = format_address(row)['adresse']
    location = geolocator.geocode(formatted_address)
    if geocode_counter % 10 == 0:
        print(f"Geocoded {geocode_counter} addresses.")
    time.sleep(1)  # Sleep for 1 second every 10 requests to avoid Nominatim rate limits
    if location:
        row['latitude'], row['longitude'] = location.latitude, location.longitude
    else:
        row['latitude'], row['longitude'] = None, None


def geocode_data(
    file_path=f'{PATH_ROOT}/files/{DOWNLOAD_FILE_NAME}',
    save_path=f'{PATH_ROOT}/files/geocoded_{DOWNLOAD_FILE_NAME}',
    save_data=True,
):
    data = pd.read_csv(file_path).to_dict(orient='records')

    # Initialize the geocode counter
    geocode_counter = 0

    # Loop over each row in the list of dictionaries
    for row in data:
        try:
            get_lat_long(row, geocode_counter)
            geocode_counter += 1
        except Exception as e:
            print(f"Error processing row: {e}")

    # Convert the list of dictionaries back to a DataFrame
    df = pd.DataFrame(data)

    if save_data:
        df.to_csv(save_path, index=False)

    # Display the final number of geocoded addresses
    print(f"Geocoded {geocode_counter} addresses.")


def format_address(row):
    row['adresse'] = re.sub(r"\s*\([^)]*\)", "", row['adresse'])  # Adjusted regex pattern
    if row['adresse'].count(',') > 1:
        row['adresse'] = f"{row['adresse'].split(',')[0]}, {row['adresse'].split(',')[-1]}"
    return row


def process_data(
    file_path=f'{PATH_ROOT}/files/geocoded_{DOWNLOAD_FILE_NAME}',
    save_path=f'{PATH_ROOT}/files/new_{DOWNLOAD_FILE_NAME}',
    save_data=True,
):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    df = df.apply(format_address, axis=1)
    # Extract the first image URL
    df['image-url-src'] = df['image-url-src'].apply(
        lambda x: x.split()[0] if not pd.isna(x) else None
    )
    df.apply(lambda x: pd.to_numeric(x, errors="ignore"))  # type: ignore

    # Convert specified columns to numeric
    numeric_columns = NUMERIC_COLUMNS
    for col in numeric_columns:
        df[col] = df[col].apply(average_hyphenated_values)

    df[numeric_columns] = (
        df[numeric_columns]
        .replace('[^\d.]', '', regex=True)
        .apply(pd.to_numeric, errors='coerce')
        .round(0)
        .astype('Int64')
    )

    # Extract 'energiklasse' letter and color into separate columns
    energimerking_parts = df['energiklasse'].str.extract('^([A-G]) - ([\w\s]+)$')

    # Assign the extracted parts to new columns in the DataFrame
    df['energiklasse'] = energimerking_parts[0]  # The letter grade
    df['energiklasse-farge'] = energimerking_parts[1]  # The color description

    # Apply try-except block for 'fasiliteter'
    try:
        df['fasiliteter'] = df['fasiliteter'].apply(
            lambda x: (
                [
                    item.text
                    for item in BeautifulSoup(x, 'html.parser').find_all(
                        'div', class_='py-4 break-words'
                    )
                ]
                if pd.notna(x)
                else []
            )
        )
        # After converting to text, check for "utvidelsesmuligheter" in the string
    except Exception as e:
        print(f"Error processing 'fasiliteter': {e}")

    df['heis'] = df['fasiliteter'].apply(lambda x: 'Heis' in x)
    df['balkong'] = df['fasiliteter'].apply(lambda x: 'Balkong/Terrasse' in x)
    df['pris/m2'] = round(df['pris'] / df['internt-bruksareal'])
    df['prisantydning/m2'] = round(df['prisantydning'] / df['internt-bruksareal'])
    df['solgt'] = False
    df['pin'] = False
    df['gjem'] = False
    # TODO: make sure this is where to add this
    # df['first-seen'] = time.strftime("%Y-%m-%d %H:%M:%S")

    if save_data:
        df.to_csv(save_path, index=False)

    print(f"processed data.")

    return df


def move_fresh_file_from_downloads():
    # check if the file exists in downloads. If it does, move it to the current directory and return the path
    if os.path.exists(f'{DOWNLOAD_PATH}/{DOWNLOAD_FILE_NAME}'):
        os.rename(
            f'{DOWNLOAD_PATH}/{DOWNLOAD_FILE_NAME}',
            f'{PATH_ROOT}/files/{DOWNLOAD_FILE_NAME}',
        )
        return f'{PATH_ROOT}/files/{DOWNLOAD_FILE_NAME}'
    else:
        return None


# %%


if __name__ == "__main__":
    move_fresh_file_from_downloads()
    geocode_data(
        file_path=f'{PATH_ROOT}/files/{DOWNLOAD_FILE_NAME}',
        save_path=f'{PATH_ROOT}/files/geocoded_{DOWNLOAD_FILE_NAME}',
        save_data=True,
    )
    df = process_data(
        file_path=f'{PATH_ROOT}/files/geocoded_{DOWNLOAD_FILE_NAME}',
        # file_path=f'{PATH_ROOT}/files/finn-eiendom.csv',
        save_path=f'{PATH_ROOT}/files/new_{DOWNLOAD_FILE_NAME}',
        save_data=True,
    )

    # %%
    print(df.head())
