from argparse import ArgumentParser
from pathlib import Path

import geopandas as gpd
import h3pandas  # noqa
import pandas as pd

parser = ArgumentParser()
parser.add_argument("--filename", type=str, required=True)

args = parser.parse_args()

filename = Path(args.filename)


if __name__ == "__main__":
    places = gpd.read_parquet(f"./data/processed/{filename}.parquet")
    
    ori_len = len(places)
    #drop closed places
    places = places[places.date_closed.isnull()]
    print(f"All places: {ori_len}")
    print(f"Removed {ori_len - len(places)} closed places")
    places.to_parquet(f"./data/processed/{filename}_open.parquet", index=False)
