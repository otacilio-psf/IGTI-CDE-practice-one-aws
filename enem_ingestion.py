import requests
import zipfile
import os
import shutil
import boto3
from clint.textui import progress

class EnemIngestion():
    
    def __init__(self):
        self._source_url = "https://download.inep.gov.br/microdados/microdados_enem_2019.zip"
        self._target_s3 = "datalake-ota-823120262488"

    def _extract(self):
        os.makedirs("./tmp", exist_ok=True)
        os.makedirs("./data",exist_ok=True)

        # download data
        print("Downloading the data...")
        r = requests.get(self._source_url, stream=True)
        if r.status_code == requests.codes.OK:
            total_length = int(r.headers.get('content-length'))
            with open("./tmp/microdados_enem_2019.zip", "wb") as new_file:
                for part in progress.bar(r.iter_content(chunk_size=1024), expected_size=(total_length/(1024)) + 1):
                    new_file.write(part)
        else:
            r.raise_for_status()
        
        # unzip data
        print("Unzip the data...")
        with zipfile.ZipFile("./tmp/microdados_enem_2019.zip", 'r') as zip_data:
            zip_data.extract("DADOS/MICRODADOS_ENEM_2019.csv", "./data")

        # delete tmp (depends where will run)
        # shutil.rmtree("./tmp", ignore_errors=True)

    def _load(self):
        file_path = "./data/DADOS"
        file_name = "MICRODADOS_ENEM_2019.csv"

        # upload file
        print(f"Upload {file_name} to S3")
        s3_client = boto3.client('s3')
        s3_client.upload_file(f'{file_path}/{file_name}',
                              self._target_s3,
                              f'raw-data/enem/{file_name}')

        # delete path (depends where will run)
        # shutil.rmtree(file_path, ignore_errors=True)

    def start(self):
        self._extract()
        self._load()
        print("Ingestion completed")

if __name__ == "__main__":
    EnemIngestion().start()
