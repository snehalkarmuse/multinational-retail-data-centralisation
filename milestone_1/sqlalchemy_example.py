import psycopg2
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'password'
DATABASE = 'Pagila'
PORT = 5432

with psycopg2.connect(host=HOST, user=USER, password = PASSWORD, dbname=DATABASE, port=PORT) as conn:
    with conn.cursor() as cur:
        cur.execute(''' create table only_samuel_movies(
        Title char,
        Year int
        )
        ''')
        print(type(cur))
        #records = cur.fetchall()
        #print(records)