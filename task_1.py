import concurrent.futures
import csv
import os

import requests
from PIL import Image
from io import BytesIO
from loguru import logger

from config import save_scv_t1, timed, open_csv, get_service_sacc
from creds import sheet_id

sheet = get_service_sacc().spreadsheets()


def load_url(url: str, timeout: int) -> requests.models.Response:
    return requests.get(url, timeout=timeout)


def get_responses(directory: str, filename: str) -> list:
    responses = []
    counter = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        future_to_url = {executor.submit(load_url, url, 60): url for url in open_csv(directory, filename)}
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                counter += 1
                if counter % 1000 == 0:
                    logger.debug(f'{counter}')

                response = future.result()
                if response.status_code == 200:
                    responses.append(response)
                else:
                    logger.warning(f'[NO_REQUEST]: {response.status_code} | {response.url}')
            except Exception as ex:
                logger.error(response.url, response.status_code, str(ex))
    return responses


def save_csv_to_gs(directory, filename, list_name):
    body = {}
    row_gs = []
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), directory, filename), "r") as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        for i in reader:
            row_gs.append(i)
    body['values'] = row_gs
    return sheet.values().update(
        spreadsheetId=sheet_id,
        range=list_name,
        valueInputOption='RAW',
        body=body).execute()


@timed
def main() -> None:
    for response in get_responses('datasets', 'image_uls.csv'):
        try:
            image = Image.open(BytesIO(response.content))
            save_scv_t1((response.url, image.size), 'results', 'task_1_image_size.csv')
        except Exception as ex:
            logger.error(response.url, response.status_code, str(ex))


if __name__ == '__main__':
    main()
    save_csv_to_gs('results', 'task_1_image_size.csv', 'main')
