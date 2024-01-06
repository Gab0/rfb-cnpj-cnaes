#!/bin/python
#

import requests
import os

import zipfile

base_url = "https://dadosabertos.rfb.gov.br/CNPJ/"

DATASET_DIR = "dataset"


def download_file(url: str, local_filename: str):
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    return local_filename


def download_data():
    N = 10
    datasets = {
        "Empresas": N,
        "Socios": N,
        "Estabelecimentos": N
    }

    for nome, num in datasets.items():
        for n in range(num):
            filename = f"{nome}{n}.zip"

            URL = base_url + filename
            local_filename = os.path.join(DATASET_DIR, URL.split('/')[-1])
            if not os.path.isfile(local_filename):
                print(f"Downloading {local_filename}...")
                download_file(URL, local_filename)

            with zipfile.ZipFile(local_filename, 'r') as zip_ref:

                print(f"Unzipping {local_filename}...")
                for info in zip_ref.infolist():
                    extracted_path = zip_ref.extract(info, path=DATASET_DIR)
                    os.rename(
                        extracted_path,
                        os.path.splitext(local_filename)[0] + ".csv"
                    )


if __name__ == "__main__":
    if not os.path.isdir(DATASET_DIR):
        os.mkdir(DATASET_DIR)
    download_data()
