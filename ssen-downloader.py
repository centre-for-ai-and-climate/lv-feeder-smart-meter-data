import json
import requests
import signal
import sys
import os

def signal_handler(sig, frame):
    print('Canceling downloads')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Read ssen-files.json and extract urls from the downloadLink property of the json array
urls = []
with open('data/raw/ssen/ssen-files.json') as f:
    urls = [x['downloadLink'] for x in json.load(f)]

print(f"Downloading {len(urls)} files from SSEN")

with requests.Session() as s:
    for url in urls:
        filename = url.split('/')[-1]
        output = f"data/raw/ssen/{filename}"

        # skip if output file exists
        if os.path.exists(output):
            print(f"Skipping {url} as {output} already exists")
            continue
        else:
            print(f"Downloading {url} into {output}")

        try:
            with s.get(url, stream=True) as r:
                r.raise_for_status()
                with open(output, 'wb') as f:
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded_size = 0
                    for chunk in r.iter_content(chunk_size=8192):
                        downloaded_size += len(chunk)
                        progress = int(downloaded_size / total_size * 100)
                        print(f"Downloading... {progress}% complete", end="\r")
                        f.write(chunk)
        except Exception as e:
            print(f"Failed to download {url} due to {e}")
            continue
