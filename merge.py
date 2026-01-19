import pandas as pd
import geopandas as gpd
from clean_data import geocode_data, NUMERIC_COLUMNS

import time


def merge_dataframes(
    existing_dataframe,
    new_dataframe,
    merged_file_path='./files/merged_finn_eiendom',
    unique_columns=['annonse-href'],
):
    """
    Processes, merges two DataFrames, ensures all attributes are retained in case of duplicates,
    prints rows missing in the new DataFrame, and saves new entries to a separate file.

    Parameters:
    - existing_file_path: Path to the existing processed CSV file.
    - new_file_path: Path to the new CSV file to process.
    - unique_columns: List of column names to use for identifying duplicates.

    Returns:
    - merged_df: The merged DataFrame with updated entries.
    """

    new_df = new_dataframe.copy()

    existing_df = existing_dataframe.copy()

    # TODO: do I need this?
    # go through the columns in NUMERIC_COLUMNS and ensure they are ints in both DataFrames if they exist
    for col in NUMERIC_COLUMNS:
        if col in existing_df.columns:
            existing_df[col] = (
                existing_df[col].apply(pd.to_numeric, errors='coerce').round(0).astype('Int64')
            )
        if col in new_df.columns:
            new_df[col] = new_df[col].apply(pd.to_numeric, errors='coerce').round(0).astype('Int64')

    # remove the columns __area and __length from existing_df if they exist
    if '__area' in existing_df.columns:
        existing_df.drop(columns=['__area', '__length'], inplace=True)

    if existing_df.empty:
        new_df.to_csv(merged_file_path + '.csv', index=False)
        # save the merged DataFrame as geojson
        new_df = preprocess_gdf_for_geojson(new_df)
        gdf = gpd.GeoDataFrame(
            new_df, geometry=gpd.points_from_xy(new_df.longitude, new_df.latitude)
        )
        gdf.to_file(merged_file_path + ".geojson", driver='GeoJSON')
        print(f"Updated dataset saved to {merged_file_path}.csv and .geojson.")
        return new_df

    if new_df.empty:
        existing_df.to_csv(merged_file_path + '.csv', index=False)
        # save the merged DataFrame as geojson
        existing_df = preprocess_gdf_for_geojson(existing_df)
        gdf = gpd.GeoDataFrame(
            existing_df, geometry=gpd.points_from_xy(existing_df.longitude, existing_df.latitude)
        )
        gdf.to_file(merged_file_path + ".geojson", driver='GeoJSON')
        print(f"Updated dataset saved to {merged_file_path}.csv and .geojson.")
        return existing_df

    # Set index to unique_columns for both DataFrames to use combine_first or merge properly
    existing_df.set_index(unique_columns, inplace=True)
    new_df.set_index(unique_columns, inplace=True)

    # Identify old unique entries not in new DataFrame
    old_unique = existing_df[~existing_df.index.isin(new_df.index)].copy()
    old_unique['solgt'] = True

    merged_df = existing_df.combine_first(new_df)

    # Update the merged_df with the modified old_unique entries
    merged_df.update(old_unique)

    merged_df.reset_index(inplace=True)
    merged_df.drop_duplicates(subset=unique_columns, keep='last', inplace=True)

    if 'first-seen' not in merged_df.columns:
        merged_df['first-seen'] = time.strftime("%Y-%m-%d %H:%M:%S")
    else:
        merged_df['first-seen'] = merged_df['first-seen'].fillna(time.strftime("%Y-%m-%d %H:%M:%S"))

    merged_df = merged_df.drop_duplicates(subset=unique_columns)

    # TODO: remove this if we do not geocode
    # from 1 to 10 delete columns named longitude.x and latitude.x if they exist
    for i in range(1, 11):
        if f'longitude.{i}' in merged_df.columns:
            merged_df.drop(columns=[f'longitude.{i}', f'latitude.{i}'], inplace=True)

    # split out entries without coordinates
    no_coords = merged_df[merged_df['longitude'].isnull()]
    if not no_coords.empty:
        print(f"{len(no_coords)} entries are missing coordinates. Saving to separate file.")
        no_coords.to_csv(merged_file_path + '_no_coords.csv', index=False)

    merged_df = merged_df.dropna(subset=['longitude', 'latitude'])

    merged_df.to_csv(merged_file_path + '.csv', index=False)

    preprocessed_df = preprocess_gdf_for_geojson(merged_df)
    gdf = gpd.GeoDataFrame(
        preprocessed_df,
        geometry=gpd.points_from_xy(preprocessed_df.longitude, preprocessed_df.latitude),
    )
    gdf.to_file(merged_file_path + ".geojson", driver='GeoJSON')

    print(f"Updated dataset saved to {merged_file_path}.csv and .geojson.")

    return merged_df


def df_to_geojson(df, output_path):
    """Convert a DataFrame with longitude/latitude columns to GeoJSON."""
    preprocessed_df = preprocess_gdf_for_geojson(df.copy())
    # Convert nullable dtypes to standard numpy dtypes for GeoJSON compatibility
    for col in preprocessed_df.columns:
        if preprocessed_df[col].dtype.name in ('Int64', 'Float64', 'boolean'):
            preprocessed_df[col] = preprocessed_df[col].astype(object).where(preprocessed_df[col].notna(), None)
    gdf = gpd.GeoDataFrame(
        preprocessed_df,
        geometry=gpd.points_from_xy(preprocessed_df.longitude, preprocessed_df.latitude),
    )
    gdf.to_file(output_path + ".geojson", driver='GeoJSON')
    print(f"Saved to {output_path}.geojson")


def preprocess_gdf_for_geojson(gdf):
    """
    Preprocess a GeoDataFrame to ensure it can be saved to GeoJSON format by converting
    lists to strings and handling other necessary data transformations.

    Parameters:
    - gdf: The GeoDataFrame to preprocess.

    Returns:
    - gdf: The preprocessed GeoDataFrame.
    """
    print("Preprocessing GeoDataFrame for GeoJSON export...")
    for column in gdf.columns:
        # Check if any element in the column is a list
        if any(isinstance(x, list) for x in gdf[column]):
            print(f"Column '{column}' contains lists. Converting to strings.")
            gdf[column] = gdf[column].apply(
                lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
            )

    return gdf
