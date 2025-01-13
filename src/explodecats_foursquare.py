from argparse import ArgumentParser
from pathlib import Path
import geopandas as gpd
import pandas as pd


#A single location can have multiple fourquare cats. This code explodes the list,
#Which creates a row for each cat code , these can then be filtered by cat.

#The codes each respond to a hierachy of cats, see (https://docs.foursquare.com/data-products/docs/places-os-data-schema
#We join with the cat schema to unpack the levels
parser = ArgumentParser()
parser.add_argument("--filename", type=str, required=True)
args = parser.parse_args()
filename = Path(args.filename)


if __name__ == "__main__":
    places = gpd.read_parquet(f"./data/processed/{filename}.parquet")
    # Exploding the 'fsq_category_ids' column
    places_exploded = places.explode('fsq_category_ids').reset_index(drop=True)
    # Rename column to sing
    places_exploded = places_exploded.rename(columns={'fsq_category_ids': 'fsq_category_id'})    

    #load the cat data
    file_path = 's3://fsq-os-places-us-east-1/release/dt=2024-12-03/categories/parquet'
    cats = pd.read_parquet(file_path, storage_options={"anon": True})
#join the two dataframes on category_id

    places_exploded = places_exploded.merge(cats, left_on='fsq_category_id', right_on='category_id', how='left')

    places_exploded.to_parquet(f"./data/processed/{filename}_admin_categories.parquet", index=False)
