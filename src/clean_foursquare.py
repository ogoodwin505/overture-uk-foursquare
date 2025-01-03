import ast
import json
from argparse import ArgumentParser
from collections.abc import Iterable
from pathlib import Path

import geopandas as gpd
import h3pandas  # noqa
import pandas as pd
from tqdm import tqdm  # Import tqdm for progress bars

# Set up progress bars globally (optional, if you want to disable it in some places)
tqdm.pandas()

parser = ArgumentParser()
parser.add_argument("--filename", type=str, required=True)

args = parser.parse_args()

filename = Path(args.filename)


def add_geometry(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Rename lat and lon
    df = df.rename(columns={"latitude": "lat", "longitude": "lng"})
    
    #check if crs is EPSG:4326 if not break
    if df.crs != "EPSG:4326":
        raise ValueError("CRS is not EPSG:4326")
    # Adding H3 indices with progress bar
    for i in tqdm(range(1, 10), desc="Adding H3 indices", unit="level"):
        df[f"h3_0{i}"] = df.h3.geo_to_h3(i).index

    return df


def add_list_cols(df: gpd.GeoDataFrame, cols: list[str]) -> gpd.GeoDataFrame:
    # Add list columns with progress bar
    for col in tqdm(cols, desc="Adding list columns", unit="column"):
        df[col] = df[col].progress_apply(
            lambda x: [] if isinstance(x, (float, type(None))) else ast.literal_eval(x)
        )
    return df


def remove_list_cols(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    list_cols = df.apply(lambda x: any(isinstance(i, list) for i in x))
    for col in tqdm(list_cols[list_cols].index, desc="Removing list columns", unit="column"):
        df[col] = df[col].astype(str)
    return df


if __name__ == "__main__":
    # Read the GeoPackage file
    print("Reading the GeoPackage file...")
    places = gpd.read_file(f"./data/raw/{filename}.gpkg")
    print(f"Data loaded from './data/raw/{filename}.gpkg'")
    # Apply the functions with progress bars
    places = add_geometry(places)
    places = add_list_cols(places, ["fsq_category_ids", "fsq_category_labels"])

    # Save the processed data to parquet file
    places.to_parquet(f"./data/processed/{filename}.parquet")

    print(f"Processing complete. Data saved to './data/processed/{filename}.parquet'")
