import click
import requests
import pandas as pd
from config import config
import psycopg2
import psycopg2.extras
import datetime



@click.command(name='btc:sync:coinmetrics')
@click.argument('start_date', required=False)
def handle(start_date=None):
    print(start_date)
    if(start_date == None):
        start_date = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime ('%Y-%m-%d')

    url = 'https://api.coinmetrics.io/v4/timeseries/asset-metrics?start_time={start_date}&page_size=10000&assets=btc&api_key={api_key}&metrics=RevHashRateNtv'.format(
        api_key=config['api.coinmetrics.io']['api_key'], start_date=start_date)
    response = requests.get(url)
    json = response.json()
    rev_has_rate_ntv = pd.DataFrame.from_records(json['data'])
    rev_has_rate_ntv['RevHashRateNtv'] = rev_has_rate_ntv['RevHashRateNtv'].astype(float).fillna(0.0)
    rev_has_rate_ntv['time'] = pd.to_datetime(rev_has_rate_ntv['time']).dt.strftime('%Y-%m-%d')
    rev_has_rate_ntv['RevHashRateNtvSec'] = rev_has_rate_ntv['RevHashRateNtv'] / 86400
    #print(rev_has_rate_ntv)

    with psycopg2.connect(**config['custom_data']) as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS rev_has_rate_ntv (date TEXT PRIMARY KEY,"
                  "rev_has_rate_ntv_day REAL);")

        insert_sql = "INSERT INTO rev_has_rate_ntv (date, rev_has_rate_ntv_day) VALUES (%s, %s)" \
                     " ON CONFLICT ON CONSTRAINT rev_has_rate_ntv_pkey DO NOTHING;"

        for index, item in rev_has_rate_ntv.iterrows():
            c.execute(insert_sql, (item['time'], item['RevHashRateNtvSec']))




