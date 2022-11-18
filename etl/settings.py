import os


class Settings:
    es_schema = os.environ.get('ES_SCHEMA')
    file_json_path = os.environ.get('FILE_JSON_PATH')
    elasticsearch_host = os.environ.get('ELASTICSEARCH_HOST')
    load_buff_size = int(os.environ.get('LOAD_BUFFER_SIZE'))
    fetch_buff = int(os.environ.get('FETCH_BUFF'))
    repeat_etl = int(os.environ.get('REPEAT_ETL'))
    dsl = {
        'dbname': os.environ.get('POSTGRES_DB'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'host': os.environ.get('POSTGRES_HOST'),
        'port': os.environ.get('POSTGRES_PORT')
    }

    class Config:
        init_json = {
            'film_work_modified': None,
            'genre_modified': None,
            'person_modified': None,
        }
