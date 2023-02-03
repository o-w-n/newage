from concurrent.futures import ThreadPoolExecutor, as_completed
import google.cloud.bigquery as bq
import pandas as pd
from config import timed

from task_1 import save_csv_to_gs

client = bq.Client.from_service_account_json('creds/serv_acc_igcr.json')
query1 = """
SELECT *
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    date BETWEEN '20170701'
    AND '20170731'
    LIMIT 100
"""
query2 = """
SELECT *
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE
    date BETWEEN '20170801'
    AND '20170830'
    LIMIT 100
"""


def create_threads():
    threads = []
    executor = ThreadPoolExecutor(5)
    for job in [client.query(query1), client.query(query2)]:
        threads.append(executor.submit(job.result))
    return threads


@timed
def get_db_responses():
    appended_data = []
    for future in as_completed(create_threads()):
        appended_data.append(future.result().to_dataframe())
    appended_data = pd.concat(appended_data)
    return appended_data


def main():
    df = get_db_responses()
    column_headers = list(df.columns.values)
    print(column_headers)
    total_trans_per_user = sum([d.get('transactions') for d in df['totals'] if d.get('transactions') is not None])
    print(total_trans_per_user)
    df.to_csv('./results/test_task_2.csv', encoding='utf-8')


if __name__ == '__main__':
    main()
    save_csv_to_gs('results', 'test_task_2.csv', 'task3_bq_dt')
