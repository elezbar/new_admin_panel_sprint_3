from typing import List


def transform(rows) -> List[dict]:
    """
    Функция для перевода сырых записей из бд в формат схемы movies
    в elasticsearch
    :param rows: Список row_dict из postgres
    :return: Список словарей
    """
    struct_data = {}
    for row in rows:
        id = row['fw_id']
        if row['fw_id'] not in struct_data:
            struct_data[id] = {}
            struct_data[id]['genre'] = set()
        struct_data[id]['id'] = id
        struct_data[id]['rating'] = row['rating']
        struct_data[id]['genre'].add(row['genre'])
        struct_data[id]['title'] = row['title']
        struct_data[id]['description'] = row['description']
        role = row['role']
        if f'{role}s' not in struct_data[id]:
            struct_data[id][f'{role}s'] = {}
        if row['id'] not in struct_data[id][f'{role}s']:
            struct_data[id][f'{role}s'][row['id']] = {}
        struct_data[id][f'{role}s'][row['id']]['id'] = row['id']
        struct_data[id][f'{role}s'][row['id']]['name'] = row['full_name']
    result_data = []
    for fw_id in struct_data:
        data = struct_data[fw_id]
        director = ''
        if data.get('directors'):
            directors = data['directors']
            director = directors[list(directors.keys())[0]]['name']
        actors_names = []
        actors = []
        writers_names = []
        writers = []
        for actor_id in data.get('actors', []):
            actors.append(
                {
                    'id': actor_id,
                    'name': data['actors'][actor_id]['name']
                }
            )
            actors_names.append(data['actors'][actor_id]['name'])
        for writer_id in data.get('writers', []):
            writers.append(
                {
                    'id': writer_id,
                    'name': data['writers'][writer_id]['name']
                }
            )
            writers_names.append(data['writers'][writer_id]['name'])
        result_data.append({
                'id': fw_id,
                'imdb_rating': data['rating'],
                'genre': list(data['genre']),
                'title': data['title'],
                'description': data['description'],
                'director': director,
                'actors_names': actors_names,
                'writers_names': writers_names,
                'actors': actors,
                'writers': writers})
    return result_data
