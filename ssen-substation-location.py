import csv
import sys
import numpy as np

substation_to_postcodes = {}
with open('data/raw/ssen/LV_FEEDER_LOOKUP.csv', 'r') as feeder_lookup:
    for row in csv.DictReader(feeder_lookup):
        postcode = row['postcode']
        if postcode == '' or postcode is None:
            continue
        key = f"{row['secondary_substation_id']}_{row['secondary_substation_name']}"
        postcodes = substation_to_postcodes.get(key, [])
        if postcode in postcodes:
            continue
        substation_to_postcodes[key] = postcodes + [postcode]

print(f"Found {len(substation_to_postcodes)} secondary substations")

postcodes_to_latlong = {}
with open('data/raw/ukpostcodes.csv', 'r') as ukpostcodes:
    for row in csv.DictReader(ukpostcodes):
        if row['latitude'] == '' or row['latitude'] is None or row['longitude'] == '' or row['longitude'] is None:
            continue
        postcodes_to_latlong[row['postcode']] = (float(row['latitude']), float(row['longitude']))

print(f"Found {len(postcodes_to_latlong)} geocoded postcodes")

substations_to_latlong = {}
for substation, postcodes in substation_to_postcodes.items():
    for postcode in postcodes:
        if postcode in postcodes_to_latlong:
            latlongs = substations_to_latlong.get(substation, [])
            substations_to_latlong[substation] = latlongs + [postcodes_to_latlong[postcode]]

print(f"Found {len(substations_to_latlong)} secondary substations with latlongs")

# Find the approximate centroid of each list of latlongs in substations_to_latlong
substations_to_centroids = {}
for substation, locations in substations_to_latlong.items():
    arr = np.array(locations)
    length = arr.shape[0]
    sum_x = np.sum(arr[:, 1])
    sum_y = np.sum(arr[:, 0])
    lon, lat = sum_x/length, sum_y/length
    substations_to_centroids[substation] = f"{lat}, {lon}"

print(f"Found {len(substations_to_centroids)} secondary substations with centroids")

# Update the SSEN data with the centroid latlongs
with open(sys.argv[1],'r') as csvinput:
    with open(sys.argv[2], 'w') as csvoutput:
        reader = csv.DictReader(csvinput)
        writer = csv.DictWriter(csvoutput, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            row['substation_geo_location'] = substations_to_centroids.get(f"{row['secondary_substation_id']}_{row['secondary_substation_name']}", None)
            writer.writerow(row)
