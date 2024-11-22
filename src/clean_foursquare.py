import ast
import json
from argparse import ArgumentParser
from collections.abc import Iterable
from pathlib import Path

import geopandas as gpd
import h3pandas  # noqa
import pandas as pd

parser = ArgumentParser()
parser.add_argument("--filename", type=str, required=True)

args = parser.parse_args()

filename = Path(args.filename)


def add_geometry(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:

       #rename lat and lon

    df = df.rename(columns={"latitude": "lat", "longitude": "lng"})
    #create geometry column
    df["geometry"] = gpd.points_from_xy(df["lng"], df["lat"])

    for i in range(1, 10):
        df[f"h3_0{i}"] = df.h3.geo_to_h3(i).index
    return df


def add_list_cols(df: gpd.GeoDataFrame, cols: list[str]) -> gpd.GeoDataFrame:
    for col in cols:
        df[col] = df[col].apply(
            lambda x: [] if isinstance(x, (float, type(None))) else ast.literal_eval(x)
        )
    return df


def remove_list_cols(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    list_cols = df.apply(lambda x: any(isinstance(i, list) for i in x))
    for col in list_cols[list_cols].index:
        df[col] = df[col].astype(str)
    return df


if __name__ == "__main__":
    places = gpd.read_file(f"./data/raw/{filename}.gpkg")

    places = add_geometry(places)
    places = add_list_cols(places, ["fsq_category_ids", "fsq_category_labels"])

    #change "facebook_id" to bigint
    places["facebook_id"] = places["facebook_id"].astype("Int64")
    places.to_parquet(f"./data/processed/{filename}.parquet")
