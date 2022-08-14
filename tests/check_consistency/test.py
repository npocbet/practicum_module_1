import os

from context_managers import postgresql_conn_context, sqlite3_conn_context

NUMBER_OF_ITEMS_PER_TRANSACTION = 100


def test_integrity_genre():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT COUNT(id) FROM content.genre;')
            res1 = postgresql_cur.fetchall()
            sqlite_cur.execute('SELECT COUNT(id) FROM genre;')
            res2 = sqlite_cur.fetchall()
            assert res1[0][0] == dict(res2[0])['COUNT(id)']


def test_integrity_person():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT COUNT(id) FROM content.person;')
            res1 = postgresql_cur.fetchall()
            sqlite_cur.execute('SELECT COUNT(id) FROM person;')
            res2 = sqlite_cur.fetchall()
            assert res1[0][0] == dict(res2[0])['COUNT(id)']


def test_integrity_filmwork():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT COUNT(id) FROM content.film_work;')
            res1 = postgresql_cur.fetchall()
            sqlite_cur.execute('SELECT COUNT(id) FROM film_work;')
            res2 = sqlite_cur.fetchall()
            assert res1[0][0] == dict(res2[0])['COUNT(id)']


def test_integrity_genre_filmwork():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT COUNT(id) FROM content.genre_film_work;')
            res1 = postgresql_cur.fetchall()
            sqlite_cur.execute('SELECT COUNT(id) FROM genre_film_work;')
            res2 = sqlite_cur.fetchall()
            assert res1[0][0] == dict(res2[0])['COUNT(id)']


def test_integrity_person_filmwork():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT COUNT(id) FROM content.person_film_work;')
            res1 = postgresql_cur.fetchall()
            sqlite_cur.execute('SELECT COUNT(id) FROM person_film_work;')
            res2 = sqlite_cur.fetchall()
            assert res1[0][0] == dict(res2[0])['COUNT(id)']


def test_contest_genre():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT id, name, description FROM content.genre ORDER BY id;')
            sqlite_cur.execute('SELECT id, name, description FROM genre ORDER BY id;')
            while True:
                records_postgresql = postgresql_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)
                records_sqlite = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records_postgresql:
                    break

                for i in range(len(records_postgresql)):
                    assert records_postgresql[i] == tuple(records_sqlite[i])


def test_contest_person():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT id, full_name FROM content.person ORDER BY id;')
            sqlite_cur.execute('SELECT id, full_name FROM person ORDER BY id;')
            while True:
                records_postgresql = postgresql_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)
                records_sqlite = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records_postgresql:
                    break

                for i in range(len(records_postgresql)):
                    assert records_postgresql[i] == tuple(records_sqlite[i])


def test_contest_film_work():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT id, title, description, creation_date, '
                                   'rating, type FROM content.film_work ORDER BY id;')
            sqlite_cur.execute('SELECT id, title, description, creation_date, '
                               'rating, type FROM film_work ORDER BY id;')
            while True:
                records_postgresql = postgresql_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)
                records_sqlite = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records_postgresql:
                    break

                for i in range(len(records_postgresql)):
                    assert records_postgresql[i] == tuple(records_sqlite[i])


def test_contest_genre_film_work():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT id, film_work_id, genre_id FROM content.genre_film_work ORDER BY id;')
            sqlite_cur.execute('SELECT id, film_work_id, genre_id FROM genre_film_work ORDER BY id;')
            while True:
                records_postgresql = postgresql_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)
                records_sqlite = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records_postgresql:
                    break

                for i in range(len(records_postgresql)):
                    assert records_postgresql[i] == tuple(records_sqlite[i])


def test_contest_person_film_work():
    with sqlite3_conn_context('db.sqlite') as sqlite3_conn:
        with postgresql_conn_context(os.environ.get('DB_USER', 'app'),
                                     os.environ.get('DB_PASSWORD', '123qwe'),
                                     os.environ.get('DB_HOST', '127.0.0.1'),
                                     "movies_db") as postgresql_conn:
            postgresql_cur = postgresql_conn.cursor()
            sqlite_cur = sqlite3_conn.cursor()
            postgresql_cur.execute('SELECT id, film_work_id, person_id, role FROM content.person_film_work ORDER BY id;')
            sqlite_cur.execute('SELECT id, film_work_id, person_id, role FROM person_film_work ORDER BY id;')
            while True:
                records_postgresql = postgresql_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)
                records_sqlite = sqlite_cur.fetchmany(size=NUMBER_OF_ITEMS_PER_TRANSACTION)

                if not records_postgresql:
                    break

                for i in range(len(records_postgresql)):
                    assert records_postgresql[i] == tuple(records_sqlite[i])
