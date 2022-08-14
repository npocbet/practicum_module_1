import os
from datetime import datetime

from sqlite_to_postgres.context_managers import sqlite3_conn_context, postgresql_conn_context
from sqlite_to_postgres.my_dataclasses import GenreDc, PersonDc, FilmWorkDc, GenreFilmworkDc, PersonFilmworkDc

NUMBER_OF_ITEMS_PER_TRANSACTION = 100

if __name__ == '__main__':
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            # delete all data from postgresql
            postgresql_cur = postgresql_conn.cursor()
            postgresql_cur.execute('TRUNCATE content.genre CASCADE;')
            postgresql_cur.execute('TRUNCATE content.person CASCADE;')
            postgresql_cur.execute('TRUNCATE content.film_work CASCADE;')
            postgresql_cur.execute('TRUNCATE content.genre_film_work CASCADE;')
            postgresql_cur.execute('TRUNCATE content.person_film_work CASCADE;')
            postgresql_conn.commit()

            sqlite_cur = sqlite3_conn.cursor()

            # genre
            sqlite_cur.execute("SELECT * FROM genre;")
            while True:
                records = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records:
                    break

                for record in records:
                    item = GenreDc(name=record['name'], description=record['description'], id=record['id'])
                    insert_query = f""" INSERT INTO content.genre (id, name, 
                    description, 
                    created, modified)
                    VALUES ('{item.id}', '{item.name}', 
                    {"'" + item.description + "'" if item.description is not None else "NULL"},
                     '{datetime.now()}', '{datetime.now()}');
                    """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()

            # person
            sqlite_cur.execute("SELECT * FROM person;")
            while True:
                records = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records:
                    break

                for record in records:
                    item = PersonDc(full_name=record['full_name'], id=record['id'])
                    insert_query = f""" INSERT INTO content.person (id, full_name, created, modified)
                    VALUES ('{item.id}', '{item.full_name.replace("'", "''")}', '{datetime.now()}', '{datetime.now()}');
                    """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()

            # film_work
            sqlite_cur.execute("SELECT * FROM film_work;")
            while True:
                records = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records:
                    break

                for record in records:
                    item = FilmWorkDc(title=record['title'], description=record['description'],
                                      creation_date=record['creation_date'], rating=record['rating'],
                                      type=record['type'], id=record['id'])
                    insert_query = f""" INSERT INTO content.film_work (
                    id, title, 
                    description, 
                    creation_date,
                    rating, type, 
                    created, modified)
                    VALUES ('{item.id}', '{item.title.replace("'", "''") if item.title is not None else ""}', 
                    {"'" + item.description.replace("'", "''") + "'" if item.description is not None else "NULL"}, 
                    {"'" + str(item.creation_date) + "'" if item.creation_date is not None else "NULL"}, 
                    {"'" + str(item.rating) + "'" if item.rating is not None else "NULL"}, '{item.type}', 
                    '{datetime.now()}', '{datetime.now()}');
                    """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()

            #genre_film_work
            sqlite_cur.execute("SELECT * FROM genre_film_work;")
            while True:
                records = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records:
                    break

                for record in records:
                    item = GenreFilmworkDc(film_work=record['film_work_id'], genre=record['genre_id'], id=record['id'])
                    insert_query = f""" INSERT INTO content.genre_film_work (id, genre_id, film_work_id, created)
                                VALUES ('{item.id}', '{item.genre}', '{item.film_work}', '{datetime.now()}');
                                """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()

            #person_film_work
            sqlite_cur.execute("SELECT * FROM person_film_work;")
            while True:
                records = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records:
                    break

                for record in records:
                    item = PersonFilmworkDc(film_work=record['film_work_id'], person=record['person_id'],
                                            role=record['role'], id=record['id'])
                    insert_query = f""" INSERT INTO content.person_film_work (id, person_id, film_work_id, 
                                role, created)
                                VALUES ('{item.id}', '{item.person}', '{item.film_work}', 
                                '{item.role}', '{datetime.now()}');
                                """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()
