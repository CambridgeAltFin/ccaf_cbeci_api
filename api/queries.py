from extensions import cache
from config import config
import psycopg2
import psycopg2.extras

@cache.cached(key_prefix='all_mining_countries')
def get_mining_countries():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('SELECT * FROM mining_area_countries ORDER BY id')
        return cursor.fetchall()

@cache.cached(key_prefix='all_mining_provinces')
def get_mining_provinces():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('SELECT * FROM mining_area_provinces ORDER BY id')
        return cursor.fetchall()