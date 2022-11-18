
from psycopg2 import OperationalError

from settings import Settings
from db import insert_temp_uuid
from utils import backoff, JsonFileStorage


@backoff(excps=(OperationalError))
def extract_film_work(conn):
    """
    Функция сбора данных для elasticsearch по последним изменённиям film_work
    :param conn: коннект к бд
    :return: Ответ бд
    """
    select_film_work = """
        select
            distinct
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id,
            p.full_name,
            g.name genre
        from content.film_work fw
        left join content.person_film_work pfw on pfw.film_work_id = fw.id
        left join content.person p on p.id = pfw.person_id
        left join content.genre_film_work gfw on gfw.film_work_id = fw.id
        left join content.genre g on g.id = gfw.genre_id
        {}
        order by fw.modified
    """
    cursor = conn.cursor()
    json_storage = JsonFileStorage(init_json=Settings.Config.init_json,
                                   file_path=Settings.file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('film_work_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where fw.modified > '{modified}'"
    cursor.execute(select_film_work.format(filter))
    while rows := cursor.fetchmany(Settings.fetch_buff):
        result = {
            'save_to_file': {
                'name': 'film_work',
                'modified': str(rows[-1]['modified'])
            },
            'rows': rows
        }
        yield result


@backoff(excps=(OperationalError))
def extract_genre(conn):
    """
    Функция сбора данных для elasticsearch по последним изменённиям genre
    :param conn: коннект к бд
    :return: Ответ бд
    """
    select_genre = """
        select
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            g.modified,
            pfw.role,
            p.id,
            p.full_name,
            g.name genre
        from content.genre g
        join content.genre_film_work gfw on gfw.genre_id = g.id
        join content.film_work fw on fw.id = gfw.film_work_id
        left join content.person_film_work pfw on pfw.film_work_id = fw.id
        left join content.person p on p.id = pfw.person_id
        {}
        order by g.modified
    """
    cursor = conn.cursor()
    json_storage = JsonFileStorage(init_json=Settings.Config.init_json,
                                   file_path=Settings.file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('genre_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where g.modified > '{modified}'"
    cursor.execute(select_genre.format(filter))
    while rows := cursor.fetchmany(Settings.fetch_buff):
        result = {
            'save_to_file': {
                'name': 'genre',
                'modified': str(rows[-1]['modified'])
            },
            'rows': rows
        }
        yield result


@backoff(excps=(OperationalError))
def extract_person(conn):
    """
    Функция сбора данных для elasticsearch по последним изменённиям person
    :param conn: коннект к бд
    :return: Ответ бд
    """
    select_person = """
        select
            pfw.film_work_id,
            p.modified
        FROM content.person p
        join content.person_film_work pfw on pfw.person_id = p.id
        {}
    """
    result = {}
    cursor = conn.cursor()
    json_storage = JsonFileStorage(init_json=Settings.Config.init_json,
                                   file_path=Settings.file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('person_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where p.modified > '{modified}'"
    cursor.execute(select_person.format(filter))
    while rows := cursor.fetchmany(Settings.fetch_buff):
        cursor_film_work = conn.cursor()
        result = {
            'save_to_file': {
                'name': 'person',
                'modified': str(rows[-1]['modified'])
            }
        }
        ids_update_film_work = set(row['film_work_id'] for row in rows)
        insert = insert_temp_uuid(cursor_film_work,
                                  'film_work_id',
                                  ids_update_film_work)
        select_film_work = """
            select
                fw.id as fw_id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                pfw.role,
                p.id,
                p.full_name,
                g.name genre
            from content.film_work fw
            left join content.person_film_work pfw on pfw.film_work_id = fw.id
            left join content.person p on p.id = pfw.person_id
            left join content.genre_film_work gfw on gfw.film_work_id = fw.id
            left join content.genre g on g.id = gfw.genre_id
            where fw.id in ({})
        """
        cursor_film_work.execute(select_film_work.format(insert))
        result['rows'] = cursor_film_work.fetchall()
        cursor_film_work.close()
        yield result
