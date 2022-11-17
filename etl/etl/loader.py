import json
import requests
from string import Template
from typing import List
from utils import backoff
from settings import elasticsearch_host, es_schema
from requests.exceptions import ConnectionError


@backoff(excps=(ConnectionError))
def load_to_es(list_data: List[dict], size_bulk: int):
    template = Template('''{"index": {"_index": "movies", "_id": "$id"}}
$data
''')
    """
    Функция отправки данных пачками в elasticsearch
    :param list_data: список данных
    :param size_bulk: размер пачки
    """
    req = requests.get(f'http://{elasticsearch_host}/_aliases').json()
    print(req)
    if 'movies' not in req:
        with open(es_schema, 'r') as f:
            requests.put(
                f'http://{elasticsearch_host}/movies',
                data=f.read(),
                headers={'Content-Type': 'application/json'})

    url_bulk = f'http://{elasticsearch_host}/_bulk?filter_path=items.*.error'
    headers = {'Content-Type': 'application/x-ndjson'}
    iter_bulk = 0
    while data_bulk := list_data[iter_bulk*size_bulk:(iter_bulk+1)*size_bulk]:
        data_body = ''
        for data in data_bulk:
            data_body += template.substitute(
                id=data['id'],
                data=json.dumps(data)
            )
        res = requests.post(url_bulk, data=data_body, headers=headers)
        iter_bulk += 1
