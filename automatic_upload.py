import json
import requests
from dotenv import load_dotenv
import os
from file_converter import geojson_to_csv
import pandas as pd
import zipfile
import io

from clean_data import geocode_data, process_data
from merge import merge_dataframes

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


def get_dataset_features(datasetId, headers):
    new_headers = headers
    new_headers['Content-Type'] = 'application/json'
    url = 'https://gis-api.atlas.co'
    reqUrl = f"{url}/datasets/vector/{datasetId}/features"
    response = requests.request("GET", reqUrl, headers=new_headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def delete_existing_dataset_features(datasetId, headers):
    dataset = get_dataset_features(datasetId, headers)
    feature_ids = extract_feature_ids(dataset)
    delete_dict = {datasetId: feature_ids}
    url = 'https://gis-api.atlas.co'
    reqUrl = f"{url}/datasets/vector/delete_features"

    print(f"Deleting {len(feature_ids)} features from dataset {datasetId}...")
    response = requests.request("DELETE", reqUrl, json=delete_dict, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def load_fc_from_geojson(geojson_file_path):
    with open(geojson_file_path) as f:
        data = json.load(f)
    return data


def upload_dataset_file(file_path, dataset_id, headers):
    url = 'https://gis-api.atlas.co'
    reqUrl = f"{url}/datasets/vector/{dataset_id}/features"
    data = load_fc_from_geojson(file_path)
    print(f"Uploading {len(data['features'])} features to dataset {dataset_id}...")
    response = requests.request("POST", reqUrl, json=data, headers=headers)
    print(response.status_code)
    if response.status_code == 200:
        print("Upload successful.")
        return response.json()
    else:
        raise Exception("Upload failed.")
        return None


def move_fresh_file_from_downloads():
    # check if the file exists in downloads. If it does, move it to the current directory and return the path
    if os.path.exists('/Users/erlingolweus/Downloads/finn-eiendom.csv'):
        os.rename(
            '/Users/erlingolweus/Downloads/finn-eiendom.csv',
            f'{PATH_ROOT}/files/finn-eiendom.csv',
        )
        return f'{PATH_ROOT}/files/finn-eiendom.csv'
    else:
        return None


def extract_feature_ids(dataset):
    return [feature['id'] for feature in dataset['geometries']['features']]


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
            delete_existing_dataset_features(dataset_id, headers)
            upload_dataset_file(
                f'{PATH_ROOT}/files/merged_finn_eiendom.geojson', dataset_id, headers
            )
            return
        else:
            print("The source file is newer than the output file. Geocoding the data...")
            fresh_df = geocode_and_process(source_file_path, source_mod_time, geocoded_data_path)
    else:
        print("No output file detected. Geocoding the data...")
        fresh_df = geocode_and_process(source_file_path, source_mod_time, geocoded_data_path)
        # georeference the new data

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

    # delete existing features
    delete_existing_dataset_features(dataset_id, headers)

    # upload the merged file
    upload_dataset_file(f'{PATH_ROOT}/files/merged_finn_eiendom.geojson', dataset_id, headers)


if __name__ == '__main__':
    load_dotenv('.env', override=True)
    main()
