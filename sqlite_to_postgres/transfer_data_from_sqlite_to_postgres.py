import os
from sqlite_to_postgres.context_managers import sqlite3_conn_context, postgresql_conn_context


def prepare_item(item):
    if item is None:
        return 'NULL'
    item = str(item)
    item = item.replace("'", "''")
    return "'" + item + "'"


NUMBER_OF_ITEMS_PER_TRANSACTION = 100

TABLES = [
    {'pg_name': 'content.genre', 'sl3_name': 'genre',
     'pg_fields': ['id', 'name', 'description', 'created', 'modified'],
     'sl3_fields': ['id', 'name', 'description', 'created_at', 'updated_at']},
    {'pg_name': 'content.person', 'sl3_name': 'person',
     'pg_fields': ['id', 'full_name', 'created', 'modified'],
     'sl3_fields': ['id', 'full_name', 'created_at', 'updated_at']},
    {'pg_name': 'content.film_work', 'sl3_name': 'film_work',
     'pg_fields': ['id', 'title', 'description', 'creation_date',
                   'rating', 'type', 'created', 'modified'],
     'sl3_fields': ['id', 'title', 'description', 'creation_date',
                    'rating', 'type', 'created_at', 'updated_at']},
    {'pg_name': 'content.genre_film_work', 'sl3_name': 'genre_film_work',
     'pg_fields': ['id', 'genre_id', 'film_work_id', 'created'],
     'sl3_fields': ['id', 'genre_id', 'film_work_id', 'created_at']},
    {'pg_name': 'content.person_film_work', 'sl3_name': 'person_film_work',
     'pg_fields': ['id', 'person_id', 'film_work_id', 'role', 'created'],
     'sl3_fields': ['id', 'person_id', 'film_work_id', 'role', 'created_at']}
]
if __name__ == '__main__':
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()

            # delete all data from postgresql
            for table in TABLES:
                postgresql_cur.execute(f'TRUNCATE {table["pg_name"]} CASCADE;')
                postgresql_conn.commit()
            # insert all data from sqlite to postgresql
            for table in TABLES:
                sqlite_cur.execute(f"SELECT {', '.join(table['sl3_fields'])} FROM {table['sl3_name']};")
                while records := sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION):

                    if not records:
                        break

                    insert_query = f""" INSERT INTO {table['pg_name']} ({', '.join(table['pg_fields'])})
                                    VALUES ({'), ('.join([", ".join(map(prepare_item, list(record))) for record in records])});
                                    """
                    postgresql_cur.execute(insert_query)
                    postgresql_conn.commit()
