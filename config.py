import os
import csv
import time
import json

from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver

logger.add(
    f'{os.getcwd()}\\log\\logs.log ',
    format=' {level} | {time:MM-DD-YY | HH:mm:ss | dddd} | {message}',
    # level='ERROR',
    rotation='1 day',
    compression='zip',
    # serialize=True
)


def create_driver() -> WebDriver:
    chrome_options = webdriver.ChromeOptions()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--lang=en")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument('ignore-certificate-errors')
    # chrome_options.add_argument(f"user-agent={useragent.random}")
    chrome_options.add_argument("--disable-site-isolation-trials")
    return webdriver.Chrome(options=chrome_options)


def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = "[{name}]: Time spend: {elapsed:.2f}s".format(
            name=func.__name__.upper(),
            elapsed=time.time() - start
        )
        logger.success(duration)
        return result

    return wrapper


def open_csv(directory: str, file_name: str) -> list[str, str]:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), directory, file_name), "r") as fp:
        reader = csv.reader(fp, delimiter=",", quotechar='"')
        return [row[0] for row in reader if 'http' in row[0]]


def save_json(data: dict, directory, file_name) -> None:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), directory, file_name), 'w',
              encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=3)


def save_scv_t1(data: list, directory: str, file_name: str) -> None:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), directory, file_name), 'a',
              newline='') as csv_file:
        writer = csv.writer(csv_file)
        if data:
            writer.writerow((data[0], f'{data[1][0]}x{data[1][1]}'))
        else:
            logger.warning(data)


def save_csv_t3(data: dict, directory: str, file_name: str) -> None:
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), directory, file_name), 'w',
              encoding='utf-8', newline='') as csv_file:
        for company_id, value in data.items():
            writer = csv.writer(csv_file)
            writer.writerow(
                (
                    company_id,
                    value.get('name'),
                    value.get('region'),
                    value.get('area'),
                    value.get('date'),
                    value.get('url')
                )
            )
