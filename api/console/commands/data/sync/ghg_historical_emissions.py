import click
import psycopg2
from psycopg2.extras import execute_values

from config import config

from api.climatewatchdata import ClimateWatchData, Gas, Sector, Source


@click.command(name='data:sync:ghg-historical-emissions')
def handle():
    climate_watch_data = ClimateWatchData()

    data = climate_watch_data.emissions(
        gas=Gas.ALL_GHG,
        sector=Sector.TOTAL_INCL_LUCF,
        source=Source.CAIT
    )

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        cursor.execute('SELECT id, code3 as code FROM countries')
        countries = cursor.fetchall()

        values = []

        for loc_data in data:
            code3 = loc_data['iso_code3']
            country = next((country_id for country_id, country_name in countries if country_name == code3), None)
            for year_data in loc_data['emissions']:
                values.append((
                    country,
                    loc_data['iso_code3'],
                    loc_data['location'],
                    year_data['year'],
                    year_data['value']
                ))

        execute_values(
            cursor,
            'INSERT INTO ghg_historical_emissions (country_id, code, name, year, value) '
            'VALUES %s '
            'ON CONFLICT (code, year) DO NOTHING',
            values
        )
