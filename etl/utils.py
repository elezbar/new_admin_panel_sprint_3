from functools import wraps
import json
import logging
from time import sleep
from typing import Optional
from exceptions import BackOffException


def backoff(start_sleep_time=0.1,
            factor=2,
            border_sleep_time=10,
            excps=(BackOffException)):
    """
    Функция для повторного выполнения функции через некоторое время, если
    возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :param excps: Типы ошибок, которые будут перехватываться
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except excps as e:
                    logging.exception(e)
                    logging.info('Waiting for sleep is %d seconds.', t)
                    sleep(t)
                    t = start_sleep_time * factor if t < border_sleep_time\
                        else border_sleep_time
        return inner
    return func_wrapper


class JsonFileStorage():
    def __init__(self, init_json: dict = {}, file_path: Optional[str] = None):
        self.file_path = file_path
        try:
            f = open(file_path, 'r')
            f.close()
        except FileNotFoundError:
            with open(self.file_path, 'x') as f:
                f.write(json.dumps(init_json))

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, 'w') as f:
            raw_data = json.dumps(state)
            f.write(raw_data)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        try:
            with open(self.file_path, 'r') as f:
                data = json.loads(f.read())
                return data
        except FileNotFoundError:
            return {}
