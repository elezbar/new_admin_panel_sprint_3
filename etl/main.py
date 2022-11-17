from etl.extractor import extract_film_work, extract_genre, extract_person
from etl.loader import load_to_es
from etl.transformer import transform


if __name__ == '__main__':
    rows = extract_film_work()
    rows += extract_genre()
    rows += extract_person()
    data = transform(rows)
    load_to_es(data, 200)
