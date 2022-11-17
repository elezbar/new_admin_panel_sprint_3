import os


es_schema = os.environ.get('ES_SCHEMA')
file_json_path = os.environ.get('FILE_JSON_PATH')
elasticsearch_host = os.environ.get('ELASTICSEARCH_HOST')
dsl = {
    'dbname': os.environ.get('POSTGRES_DB'),
    'user': os.environ.get('POSTGRES_USER'),
    'password': os.environ.get('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT')
}
init_json = {
    'film_work_modified': None,
    'genre_modified': None,
    'person_modified': None,
}
