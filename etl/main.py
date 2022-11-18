from time import sleep

from schedule import every, repeat, run_pending
import requests
from db import conn_context_pg

from etl.extractor import extract_film_work, extract_genre, extract_person
from etl.loader import load_to_es
from etl.transformer import transform
from utils import backoff
from settings import Settings


@repeat(every(Settings.repeat_etl).seconds)
@conn_context_pg(Settings.dsl)
def extract_transform_load(dsl):
    """
    Функция ETL, предназначенная для выгрузки данных
    из postgres в elasticsearch
    """
    for extract in (extract_film_work, extract_genre, extract_person):
        for extract_data in extract(dsl):
            data = transform(extract_data['rows'])
            load_to_es(data,
                       buff_size=Settings.load_buff_size,
                       save_to_file=extract_data['save_to_file'])


@backoff(excps=(requests.exceptions.ConnectionError))
def load_es_schema():
    """
    Функция проверяющая наличие схемы в elasticsearch
    и загружающая её при отсутствии
    """
    req = requests.get(f'http://{Settings.elasticsearch_host}/_aliases').json()
    if 'movies' not in req:
        with open(Settings.es_schema, 'r') as f:
            requests.put(
                f'http://{Settings.elasticsearch_host}/movies',
                data=f.read(),
                headers={'Content-Type': 'application/json'})


if __name__ == '__main__':
    load_es_schema()
    while True:
        run_pending()
        sleep(1)
