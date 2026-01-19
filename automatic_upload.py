import json
import requests
from dotenv import load_dotenv
import os
from file_converter import geojson_to_csv
import pandas as pd
import zipfile
import io

from clean_data import geocode_data, process_data
from merge import merge_dataframes, df_to_geojson

load_dotenv()

PATH_ROOT = os.getenv('PATH_ROOT')


def fetch_jwt_token(username, password):
    url = 'https://gis-api.atlas.co'
    reqUrl = f"{url}/auth/login"
    headersList = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = f"username={username}&password={password}"

    response = requests.request("POST", reqUrl, data=payload, headers=headersList)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        return None


def get_existing_dataset_file(datasetId, headers):
    url = 'https://gis-api.atlas.co'
    reqUrl = f"{url}/datasets/download/{datasetId}?format=GEOJSON"
    response = requests.request("GET", reqUrl, headers=headers)
    if response.status_code == 200:
        # Check if content is a ZIP file by looking at first few bytes
        content_start = response.content[:4]
        is_zip = content_start.startswith(b'PK\x03\x04')

        if is_zip:
            # Handle ZIP file
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.geojson'):
                        with zip_ref.open(file_name) as geojson_file:
                            return json.load(geojson_file)
        else:
            # Handle direct GeoJSON response
            return response.json()

        return None
    else:
        return None


def load_fc_from_geojson(geojson_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)
    return data


def upload_dataset_file(file_path, webhook_url):
    data = load_fc_from_geojson(file_path)
    print(f"Uploading {len(data['features'])} features via webhook...")
    response = requests.post(webhook_url, json=data)
    print(response.status_code)
    if response.status_code == 200:
        task_id = response.json().get('task_id')
        print(f"Upload successful. Task ID: {task_id}")
        return response.json()
    else:
        raise Exception(f"Upload failed: {response.text}")


def move_fresh_file_from_downloads():
    # check if the file exists in downloads. If it does, move it to the current directory and return the path
    import shutil
    download_path = os.getenv('DOWNLOAD_PATH')
    download_file_name = os.getenv('DOWNLOAD_FILE_NAME')
    source = f'{download_path}/{download_file_name}'
    dest = f'{PATH_ROOT}/files/{download_file_name}'
    if os.path.exists(source):
        try:
            shutil.copy2(source, dest)
            os.remove(source)
            print(f"Moved {source} to {dest}")
            return dest
        except PermissionError:
            try:
                shutil.copy2(source, dest)
                print(f"Copied {source} to {dest} (could not delete original)")
                return dest
            except Exception as e:
                print(f"Could not copy file from downloads: {e}")
                return None
    else:
        return None


def file_last_modified_time(file_path):
    """Get the last modified time of a file."""
    return os.path.getmtime(file_path)


def geocode_and_process(source_file_path, source_mod_time, geocoded_data_path):
    if os.path.exists(geocoded_data_path):
        if file_last_modified_time(geocoded_data_path) <= source_mod_time:
            print("Geocoded data is outdated. Regenerating...")
            geocode_data(source_file_path, geocoded_data_path, save_data=True)
        else:
            print("Geocoded data is up-to-date.")
    else:
        print("Geocoded data not found. Generating...")
        geocode_data(source_file_path, geocoded_data_path, save_data=True)

    print("Processing the geocoded data...")
    return process_data(
        geocoded_data_path, f'{PATH_ROOT}/files/new_finn_eiendom.csv', save_data=True
    )


def main():
    username = os.getenv('ATLAS_USERNAME')
    password = os.getenv('ATLAS_PASSWORD')
    dataset_id = os.getenv('DATASET_ID')
    webhook_url = os.getenv('WEBHOOK_URL')
    move_fresh_file_from_downloads()

    token = fetch_jwt_token(username, password)
    headers = {"Authorization": f"Bearer {token}"}

    source_file_path = f'{PATH_ROOT}/files/finn-eiendom.csv'
    geocoded_data_path = f'{PATH_ROOT}/files/geocoded_finn_eiendom.csv'
    output_file_path = f'{PATH_ROOT}/files/merged_finn_eiendom.geojson'

    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"No source data found at {source_file_path}.")

    source_mod_time = file_last_modified_time(source_file_path)

    if os.path.exists(output_file_path):
        output_mod_time = file_last_modified_time(output_file_path)

        if source_mod_time < output_mod_time:
            print("The data is already up-to-date. No need to process.")
            print("Uploading the existing file to Atlas...")
            upload_dataset_file(f'{PATH_ROOT}/files/merged_finn_eiendom.geojson', webhook_url)
            return
        else:
            print("The source file is newer than the output file. Geocoding the data...")
            fresh_df = geocode_and_process(source_file_path, source_mod_time, geocoded_data_path)
    else:
        print("No output file detected. Geocoding the data...")
        fresh_df = geocode_and_process(source_file_path, source_mod_time, geocoded_data_path)
        # georeference the new data

    if dataset_id:
        live_dataset_file = get_existing_dataset_file(dataset_id, headers)

        # save the existing dataset with timestamp in case we need to revert
        with open(
            f'{PATH_ROOT}/old_datasets/existing_finn_eiendom_{pd.Timestamp.now()}.geojson', 'w'
        ) as f:
            json.dump(live_dataset_file, f)

        live_dataset_path = f'{PATH_ROOT}/files/existing_finn_eiendom.csv'
        live_dataframe = geojson_to_csv(live_dataset_file, live_dataset_path)
        merged_file_path = f'{PATH_ROOT}/files/merged_finn_eiendom'
        identifying_columns = ['annonse-href']

        # merge the datasets
        print("Merging dataframes...")
        merge_dataframes(live_dataframe, fresh_df, merged_file_path, identifying_columns)

        # upload the merged file via webhook
        upload_dataset_file(f'{PATH_ROOT}/files/merged_finn_eiendom.geojson', webhook_url)
    else:
        print("No dataset ID provided. Skipping download and merge, uploading fresh data only...")
        output_path = f'{PATH_ROOT}/files/merged_finn_eiendom'
        df_to_geojson(fresh_df, output_path)
        upload_dataset_file(f'{output_path}.geojson', webhook_url)


if __name__ == '__main__':
    load_dotenv('.env', override=True)
    main()
