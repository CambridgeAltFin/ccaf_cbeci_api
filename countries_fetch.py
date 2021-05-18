import click
import psycopg2
import logging
import requests
from pprint import pformat
from config import config
from datetime import datetime

DEFAULT_LOG_LEVEL = logging.INFO
LOGGER = logging.getLogger()

def get_countries():
    def _to_item(record):
        return {
            'country': record[0],
            'code': record[1],
            'electricity_consumption': record[2],
            'country_flag': record[3],
            'series_id': record[4],
            'year': record[5]
        }

    with psycopg2.connect(**config['custom_data']) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM countries")
            return [_to_item(record) for record in cursor.fetchall()]


def get_ec_from_api(series_id):
    params = {
        'api_key': config['api_eia_gov']['api_key'],
        'series_id': series_id
    }
    response = requests.get("http://api.eia.gov/series", params=params).json()
    LOGGER.debug(f"series_id - {series_id}: Response:\n\n{pformat(response)}\n\n")

    return response


def get_latest_data(data):
    max_year = None
    value = None

    for year, year_value in data:
        try:
            y = int(year)
            v = float(year_value)
            if max_year is None or y > max_year:
                max_year = y
                value = v
        except ValueError:
            pass

    return max_year, value


def save_country_value(country, electricity_consumption, year):
    if country['electricity_consumption'] != electricity_consumption or country['year'] != year:
        with psycopg2.connect(**config['custom_data']) as connection:
            with connection.cursor() as cursor:
                table_name = 'countries'
                # Template of the query to paste a row to a table
                insert_sql = """
                    UPDATE {table_name}
                    SET electricity_consumption = %(electricity_consumption)s,
                        year = %(year)s
                    WHERE series_id = %(series_id)s
                """.format(table_name=table_name)

                try:
                    cursor.execute(insert_sql, {
                        'series_id': country['series_id'],
                        'electricity_consumption': electricity_consumption,
                        'year': year
                    })
                except Exception as error:
                    LOGGER.exception(f"{table_name}: {str(error)}")
                    return False
                finally:
                    connection.commit()
    return True


def update_country_ec(country):
    # LOGGER.info('update_country_electricity_consumption: %s' % country['country'])
    json = get_ec_from_api(country['series_id'])
    data = json['series'][0]['data']
    year, value = get_latest_data(data)
    return save_country_value(country, electricity_consumption=value, year=year)


@click.command()
@click.option('--log-level', '-l', default=DEFAULT_LOG_LEVEL)
def main(log_level):
    # Logging
    level = log_level.upper() if isinstance(log_level, str) else log_level
    LOGGER.setLevel(level)
    # Console outputs
    LOGGER.addHandler(logging.StreamHandler())

    LOGGER.info(f"countires electricity_consumption: as of {datetime.utcnow().isoformat()}")
    countries = get_countries()
    for country in countries:
        if country['series_id'] is not None:
            update_country_ec(country)


if __name__ == '__main__':
    main()
