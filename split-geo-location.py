import sys
import csv

def split_lat_lon(latlon):
    if latlon is None or latlon == '' or latlon == ', ':
        return None, None
    try:
        lat, lon = latlon.split(',')
        lat, lon = float(lat.strip()), float(lon.strip())
    except Exception as e:
        print(f'Error splitting into lat/lon: "{latlon}"')
        raise e
    return lat, lon

with open(sys.argv[1],'r') as csvinput:
    with open(sys.argv[2], 'w') as csvoutput:
        writer = csv.writer(csvoutput)
        reader = csv.reader(csvinput)

        headers = next(reader)

        writer.writerow(headers + ['substation_latitude', 'substation_longitude'])

        for row in reader:
            lat, lon = split_lat_lon(row[7])
            writer.writerow(row +[lat, lon])