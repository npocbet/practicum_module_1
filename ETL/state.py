'''
Задача на реализацию состояния для ETL
Немного пояснений.
У вас есть класс State. Его основная задача — работать с локальной копией состояния данных.
Поэтому там определены всего две функции: set_state и get_state. Через них происходит управление
состоянием. Основная «изюминка» State — восстановление состояния во время старта приложения, если такое
состояние существовало.
Для работы с постоянным хранилищем состояния используется параметр storage. У него так же определены два
метода: retrieve_state и save_state, которые отвечают за загрузку состояния из постоянного хранилища и
сохранение состояния в постоянное хранилище. Метод retrive_state должен возвращать пустой словарь, если в
хранилище нет данных.
Реализуйте методы класса State, а также создайте класс JsonFileStorage, который будет сохранять данные в
формате JSON в указанный файл (file_path) и доставать данные из этого файла. Для работы с форматом JSON
воспользуйтесь встроенной в Python библиотекой json.
'''

import abc
import json
from typing import Any, Optional


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        # дописано
        with open(self.file_path, 'w') as cat_file:
            json.dump(state, cat_file)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        # дописано
        try:
            with open(self.file_path) as cat_file:
                return json.load(cat_file)
        except ValueError as e:
            return dict()
        except FileNotFoundError as e:
            return dict()


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        # дописано
        storage_dict = self.storage.retrieve_state()
        storage_dict[key] = value
        self.storage.save_state(storage_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        # дописано
        storage_dict = self.storage.retrieve_state()
        if key not in storage_dict.keys():
            return None
        return storage_dict[key]
