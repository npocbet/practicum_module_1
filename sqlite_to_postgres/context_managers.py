import sqlite3
from contextlib import contextmanager

import psycopg2


@contextmanager
def sqlite3_conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    yield conn  # С конструкцией yield вы познакомитесь в следующем модуле
    # Пока воспринимайте её как return, после которого код может продолжить выполняться дальше
    conn.close()


@contextmanager
def postgresql_conn_context(user, password, host, database):
    conn = psycopg2.connect(user=user, password=password, host=host, database=database)
    yield conn
    conn.close()