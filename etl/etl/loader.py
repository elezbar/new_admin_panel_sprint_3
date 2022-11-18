import json
from string import Template
from typing import List

import requests

from utils import backoff, JsonFileStorage
from settings import Settings
from requests.exceptions import ConnectionError


@backoff(excps=(ConnectionError))
def load_to_es(list_data: List[dict], buff_size: int, save_to_file: dict):
    template = Template('''{"index": {"_index": "movies", "_id": "$id"}}
$data
''')
    """
    Функция отправки данных пачками в elasticsearch
    :param list_data: список данных
    :param buff_size: размер пачки
    """
    url_elast = Settings.elasticsearch_host
    url_bulk = f'http://{url_elast}/_bulk?filter_path=items.*.error'
    headers = {'Content-Type': 'application/x-ndjson'}
    iter_bulk = 0
    while data_bulk := list_data[iter_bulk*buff_size:(iter_bulk+1)*buff_size]:
        data_body = ''
        for data in data_bulk:
            data_body += template.substitute(
                id=data['id'],
                data=json.dumps(data)
            )
        requests.post(url_bulk, data=data_body, headers=headers)
        iter_bulk += 1
    json_storage = JsonFileStorage(init_json=Settings.Config.init_json,
                                   file_path=Settings.file_json_path)
    state = json_storage.retrieve_state()
    state[f"{save_to_file['name']}_modified"] = save_to_file['modified']
    json_storage.save_state(state)
