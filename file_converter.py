import json
import csv
import pandas as pd


def geojson_path_to_csv(geojson_file_path, csv_file):
    with open(geojson_file_path) as f:
        data = json.load(f)

    features = data['features']
    attribute_names = set()

    for feature in features:
        attributes = feature['properties']
        attribute_names.update(attributes.keys())

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['latitude', 'longitude'] + list(attribute_names))

        for feature in features:
            coordinates = feature['geometry']['coordinates']
            attributes = feature['properties']
            row = [coordinates[1], coordinates[0]] + [
                attributes.get(attr) for attr in attribute_names
            ]
            writer.writerow(row)


def geojson_to_csv(geojson, csv_file):

    # geojson is a dictionary
    # check if it has the required keys. If not, return an empty DataFrame
    if not geojson or 'type' not in geojson or 'features' not in geojson:
        return pd.DataFrame()

    features = geojson['features']
    attribute_names = set()

    for feature in features:
        attributes = feature['properties']
        attribute_names.update(attributes.keys())

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['latitude', 'longitude'] + list(attribute_names))

        for feature in features:
            coordinates = feature['geometry']['coordinates']
            attributes = feature['properties']
            row = [coordinates[1], coordinates[0]] + [
                attributes.get(attr) for attr in attribute_names
            ]
            writer.writerow(row)

    df = pd.read_csv(csv_file)
    return df


def csv_to_geojson(csv_file, geojson_file):
    with open(csv_file, 'r') as csv_f:
        reader = csv.reader(csv_f)
        headers = next(reader)

        with open(geojson_file, 'w') as geojson_f:
            geojson_f.write('{"type":"FeatureCollection","features":[')

            first_feature = True
            for row in reader:
                if not first_feature:
                    geojson_f.write(',')
                first_feature = False

                geojson_f.write('{"type":"Feature","geometry":{"type":"Point","coordinates":[')
                geojson_f.write(row[1] + ',' + row[0])
                geojson_f.write(']},"properties":{')
                properties = []
                for i, header in enumerate(headers[2:]):
                    # Escape double quotes in property values
                    clean_value = row[i + 2].replace('"', '\\"')
                    properties.append(f'"{header}":"{clean_value}"')
                geojson_f.write(','.join(properties))
                geojson_f.write('}}')

            geojson_f.write(']}')


# Usage example

if __name__ == '__main__':
    # geojson_to_csv('new_finn_eiendom.geojson', 'new_finn_eiendom.csv')
    csv_to_geojson('existing_finn_eiendom.csv', 'existing_finn_eiendom.geojson')
