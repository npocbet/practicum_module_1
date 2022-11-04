import os
from typing import Any

import elasticsearch
import psycopg2
import psycopg2.extras
import logging
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg2 import OperationalError

from ETL.backoff import backoff
from state import State, JsonFileStorage

START_ID = os.environ.get('START_ID', '00000000-0000-0000-0000-000000000000')
LIMIT_AT = os.environ.get('LIMIT_AT', 100)

logging.basicConfig(
    filename='etl.log',
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    level=logging.INFO
)


class ETL:
    def __init__(self):
        """
        Основной метод, который циклически считывает порцию данных из БД, преобразует их и
        записывает в Elasticsearch. Реализовано сохранение последнего обработанного индекса
        в файл.
        """
        self.current_id = None
        self.es = None
        self.storage = State(JsonFileStorage('state.json'))

    @backoff(log=logging, exception=elasticsearch.exceptions.ConnectionError)
    def main_cicle(self):
        self.es = Elasticsearch(f'{os.environ.get("ES_HOST", "localhost")}:{os.environ.get("ES_PORT", "9200")}')
        self.current_id = START_ID if self.storage.get_state('last_id') is None else self.storage.get_state('last_id')

        while True:
            db_data = self.extract(self.current_id)
            if len(db_data) == 0:
                self.current_id = START_ID
                self.transaction = 0
                continue
            bulk(self.es, self.load(db_data))
            self.current_id = db_data[-1]['id']
            self.storage.set_state('last_id', self.current_id)
            logging.info(f'transaciton added, last filmwork_id={self.current_id}')

    @backoff(log=logging, exception=OperationalError)
    def extract(self, current_id_t: str) -> list[dict[Any, Any]]:
        """
        Метод загружает порцию данных из БД и возвращает словарь
        :param current_id_t: id, начиная с которого загружаются данные
        :return: словарь данных выборки без преобразований
        """
        with psycopg2.connect(f"""dbname={os.environ.get('DB_NAME', 'movies_db')} 
            host={os.environ.get('DB_HOST', 'localhost')} user={os.environ.get('DB_USER', 'app')} 
            password={os.environ.get('DB_PASSWORD', '123qwe')}""") as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(f'''SELECT
                               fw.id,
                               fw.title,
                               fw.description,
                               fw.rating,
                               fw.type,
                               fw.created,
                               fw.modified,
                               COALESCE (
                                   json_agg(
                                       DISTINCT jsonb_build_object(
                                           'person_role', pfw.role,
                                           'person_id', p.id,
                                           'person_name', p.full_name
                                       )
                                   ) FILTER (WHERE p.id is not null),
                                   '[]'
                               ) as persons,
                               array_agg(DISTINCT g.name) as genres
                            FROM content.film_work fw
                            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                            LEFT JOIN content.person p ON p.id = pfw.person_id
                            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                            LEFT JOIN content.genre g ON g.id = gfw.genre_id
                            WHERE fw.id > '{current_id_t}'
                            GROUP BY fw.id
                            ORDER BY fw.id
                            LIMIT {LIMIT_AT}; ''')

        ans = cur.fetchall()
        ans1 = []
        for row in ans:
            ans1.append(dict(row))

        return ans1

    def transform_one_element(self, data_dict: dict) -> dict[str, str | list[Any] | Any]:
        """
        Метод преобразует словарь одной записи Postgresql в словарь документа Elasticsearch
        :param data_dict: словарь, полученный из БД
        :return: преобразованный словарь, для записи в Elasticsearch согласно схеме индекса
        """
        data = dict()
        data['id'] = data_dict['id']
        data['imdb_rating'] = data_dict['rating']
        data['genre'] = data_dict['genres']
        data['title'] = data_dict['title']
        data['description'] = data_dict['description']
        data['director'] = []
        data['actors_names'] = []
        data['writers_names'] = []
        data['actors'] = []
        data['writers'] = []
        for person in data_dict['persons']:
            if person['person_role'] == 'director':
                data['director'].append(person['person_name'])
            elif person['person_role'] == 'actor':
                data['actors_names'].append(person['person_name'])
                data['actors'].append({'id': person['person_id'], 'name': person['person_name']})
            elif person['person_role'] == 'writer':
                data['writers_names'].append(person['person_name'])
                data['writers'].append({'id': person['person_id'], 'name': person['person_name']})

        return data

    def load(self, data: list) -> None:
        """
        Метод получает на вход список данных, которые bulk-запросом отправляет в Elasticsearch
        :param data: данные для передачи
        :return: метод ничего не возвращает
        """
        for el in data:
            cur_el = self.transform_one_element(el)
            yield {
                "_index": "movies",
                "_id": cur_el['id'],
                "id": cur_el['id'],
                "imdb_rating": cur_el['imdb_rating'],
                "genre": cur_el['genre'],
                "title": cur_el['title'],
                "description": cur_el['description'],
                "director": cur_el['director'],
                "actors_names": cur_el['actors_names'],
                "writers_names": cur_el['writers_names'],
                "actors": cur_el['actors'],
                "writers": cur_el['writers'],
            }


if __name__ == '__main__':
    load_dotenv()
    etl = ETL()
    etl.main_cicle()
