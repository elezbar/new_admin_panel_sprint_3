from settings import init_json, file_json_path, dsl
from psycopg2 import OperationalError
from db import conn_context_pg, insert_temp_uuid
from utils import JsonFileStorage

from utils import backoff


@backoff(excps=(OperationalError))
@conn_context_pg(dsl)
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
    json_storage = JsonFileStorage(init_json=init_json,
                                   file_path=file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('film_work_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where fw.modified > '{modified}'"
    cursor.execute(select_film_work.format(filter))
    rows = cursor.fetchall()
    if rows:
        state['film_work_modified'] = str(rows[-1]['modified'])
        json_storage.save_state(state)
    return rows


@backoff(excps=(OperationalError))
@conn_context_pg(dsl)
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
    json_storage = JsonFileStorage(init_json=init_json,
                                   file_path=file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('genre_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where g.modified > '{modified}'"
    cursor.execute(select_genre.format(filter))
    rows = cursor.fetchall()
    if rows:
        state['genre_modified'] = str(rows[-1]['modified'])
        json_storage.save_state(state)
    return rows


@backoff(excps=(OperationalError))
@conn_context_pg(dsl)
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
    cursor = conn.cursor()
    json_storage = JsonFileStorage(init_json=init_json,
                                   file_path=file_json_path)
    state = json_storage.retrieve_state()
    modified = state.get('person_modified')
    if not modified:
        filter = ''
    else:
        filter = f"where p.modified > '{modified}'"
    cursor.execute(select_person.format(filter))
    rows = cursor.fetchall()
    if rows:
        state['person_modified'] = str(rows[-1]['modified'])
        json_storage.save_state(state)
        ids_update_film_work = set(row['film_work_id'] for row in rows)
        insert = insert_temp_uuid(cursor, 'film_work_id', ids_update_film_work)
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
        cursor.execute(select_film_work.format(insert))
        rows = cursor.fetchall()
    return rows
