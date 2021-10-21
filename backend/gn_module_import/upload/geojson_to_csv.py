import argparse
import csv
import json
import sys
import geojson
from shapely.geometry import shape

# code source : https://github.com/unacast/actions/blob/master/geojson2csv/geojson2csv.py


def parse_geojson(infile, outfile, geometry_column_name):
    try:
        with open(infile, 'r') as geojsonfile:
            input_geojson = geojson.load(geojsonfile)

        records = []
        for elm in input_geojson.features:
            geom = shape(elm['geometry']).wkt
            geom = {geometry_column_name: geom}
            prop = elm['properties']
            prop.update(geom)
            records.append(prop)

        with open(outfile, 'w') as csvfile:
            fieldnames = records[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()
            writer.writerows(records)
    except Exception:
        raise