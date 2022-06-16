import click
import psycopg2

from config import config

from api.climatewatchdata import ClimateWatchData, Gas, Sector, Source


@click.command(name='data:sync:global-historical-emissions')
def handle():
    climate_watch_data = ClimateWatchData()

    data = climate_watch_data.emissions(
        gas=Gas.ALL_GHG,
        sector=Sector.TOTAL_INCL_LUCF,
        source=Source.CAIT,
        location='WORLD'
    )

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor()

        for loc_data in data:
            for year_data in loc_data['emissions']:
                cursor.execute(
                    'INSERT INTO global_historical_emissions (code, name, year, value) '
                    'VALUES (%s, %s, %s, %s) '
                    'ON CONFLICT (code, year) DO UPDATE '
                    'SET value = %s',
                    (loc_data['iso_code3'],
                     loc_data['location'],
                     year_data['year'],
                     year_data['value'],
                     year_data['value'])
                )
