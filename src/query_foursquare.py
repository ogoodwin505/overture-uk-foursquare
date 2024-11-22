from argparse import ArgumentParser
from pathlib import Path
import duckdb

# Set up argument parser
parser = ArgumentParser()
parser.add_argument("--minx", type=float, required=True, help="Minimum longitude")
parser.add_argument("--maxx", type=float, required=True, help="Maximum longitude")
parser.add_argument("--miny", type=float, required=True, help="Minimum latitude")
parser.add_argument("--maxy", type=float, required=True, help="Maximum latitude")
parser.add_argument("--filename", type=str, required=True, help="Output file name without extension")
args = parser.parse_args()

# Set up directories
filename = Path(args.filename)
raw_dir = Path("data/raw")
processed_dir = Path("data/processed")

raw_dir.mkdir(parents=True, exist_ok=True)
processed_dir.mkdir(parents=True, exist_ok=True)

# Query to filter data and write output
query = f"""
INSTALL httpfs;
INSTALL spatial;

LOAD httpfs;
LOAD spatial;

COPY (
    SELECT
        fsq_place_id,
        name,
        latitude,
        longitude,
        address,
        locality,
        region,
        postcode,
        admin_region,
        post_town,
        tel,
        website,
        email,
        facebook_id
        instagram,
        twitter,
        CAST(fsq_category_ids AS JSON) AS fsq_category_ids,
        CAST(fsq_category_labels AS JSON) AS fsq_category_labels,
        CAST(dt AS VARCHAR) AS dt --datetime not supported
    FROM
        read_parquet('s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/*')
    WHERE
        longitude > {args.minx}
        AND longitude < {args.maxx}
        AND latitude > {args.miny}
        AND latitude < {args.maxy}
    
) TO 'data/raw/{filename}.gpkg'
WITH (FORMAT GDAL, DRIVER 'GPKG');
"""

# Run the query
duckdb.query(query)

# Clean up temporary files if they exist
rtree_file = Path(f"data/raw/{filename}.gpkg.tmp_rtree_{filename}.db")
if rtree_file.exists():
    rtree_file.unlink()

print(f"Data successfully exported to 'data/raw/{filename}.gpkg'")
