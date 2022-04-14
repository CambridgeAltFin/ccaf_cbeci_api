from extensions import cache
from config import config
import psycopg2
import psycopg2.extras


@cache.memoize()
def get_mining_countries(version=None):
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        version_subquery = version_query('mining_area_countries', version)
        cursor.execute(
            'SELECT m.*, c.code FROM mining_area_countries AS m '
            'LEFT JOIN countries AS c ON c.id = m.country_id '
            'WHERE m.version = ({version_sql}) ORDER BY m.date, m.value DESC'.format(version_sql=version_subquery[0]),
            version_subquery[1]
        )
        return cursor.fetchall()


@cache.memoize()
def get_mining_provinces(country_code, version=None):
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        version_subquery = version_query('mining_area_provinces', version)
        cursor.execute(
            'SELECT * FROM mining_area_provinces '
            'WHERE version = ({version_sql}) AND country_id = %s '
            'ORDER BY id'.format(version_sql=version_subquery[0]),
            version_subquery[1] + (get_countries_dict()[country_code.lower()],)
        )
        return cursor.fetchall()


@cache.memoize()
def get_countries_dict():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('SELECT id, code FROM countries')
        countries = cursor.fetchall()
        countries_dict = {}
        for country in countries:
            countries_dict[country['code'].lower()] = country['id']
        return countries_dict


def version_query(table, version):
    if version is None:
        sql = f'SELECT MAX(version) FROM {table}'
        binds = ()
    else:
        sql = f'SELECT DISTINCT version FROM {table} WHERE version <= %s ORDER BY version DESC LIMIT 1'
        binds = (version,)
    return sql, binds
