import time
from multiprocessing import Pool

from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By

from config import create_driver, save_json, save_csv_t3, timed

driver = create_driver()
BASE_URL = 'https://www.olx.ua/d/uk/nedvizhimost/kvartiry'


def create_pagination_list() -> list:
    urls_list = []
    driver.get(BASE_URL)
    time.sleep(2)

    page_number = driver.find_element(By.XPATH, "//div[@class='css-4mw0p4']").text.split('\n')[-1]
    if page_number.isdigit():
        urls_list.append(BASE_URL)
        for counter in range(2, int(page_number) + 1):
            urls_list.append(f'https://www.olx.ua/d/uk/nedvizhimost/kvartiry?page={counter}')
    return urls_list


def get_page_data(url) -> dict:
    companies_data = {}
    driver.get(url)
    time.sleep(3)
    for company_block in driver.find_elements(By.XPATH, "//div[@class='css-1sw7q4x']"):
        try:
            company_url = company_block.find_element(By.XPATH, './a').get_attribute('href')
            company_id = company_url.split('-')[-1].split('.')[0]
            companies_data.update({company_id: {}})
            name = company_block.find_element(By.XPATH, ".//div[@class='css-u2ayx9']/h6").text
            date_update = company_block.find_element(By.XPATH, ".//div[@class='css-1apmciz']").find_element(
                By.XPATH, ".//div[@class='css-odp1qd']").find_element(By.XPATH, ".//p").text
            area = company_block.find_element(By.XPATH, ".//div[@class='css-1apmciz']").find_element(
                By.XPATH, ".//div[@class='css-odp1qd']").find_element(By.XPATH, ".//div[@class='css-1kfqt7f']") \
                .find_element(By.XPATH, './/span').text
            price = company_block.find_element(By.XPATH, ".//div[@class='css-1apmciz']"). \
                find_element(By.XPATH, ".//div[@class='css-u2ayx9']/p").text.split('.')[0]
            date = date_update.split(' - ')[-1]
            region = date_update.split(' - ')[0]
            companies_data[company_id] = {
                'name': name,
                'region': region,
                'area': area,
                'price': price,
                'date': date,
                'url': company_url,
                'source': url
            }
        except NoSuchElementException:
            pass
    return companies_data


@timed
def main() -> dict:
    main_dict = {}
    url_list = create_pagination_list()
    counter = 0
    with Pool(processes=8) as pool:
        for prices_dict in pool.imap_unordered(get_page_data, url_list):
            main_dict.update(prices_dict)
            counter += 1
            print(f'Progress: {counter}/{len(url_list)}')
    return main_dict


if __name__ == '__main__':
    f = main()
    save_json(f, 'results', 'test_task_3.json')
    save_csv_t3(f, 'results', 'test_task_3.csv')
