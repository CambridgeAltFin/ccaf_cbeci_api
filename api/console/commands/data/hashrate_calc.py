from data_source.coinmetrics import CoinMetrics
from helpers import get_guess_consumption, get_hash_rates_by_miners_types, get_avg_effciency_by_miners_types_old, load_typed_hasrates
from console.cli import LOGGER
from config import config, DEFAULT_ELECTRICITY_PRICE

import click
import psycopg2
import pandas as pd
from datetime import datetime


@click.command(name='data:fetch:hashrate')
@click.option('--price', '-p', default=DEFAULT_ELECTRICITY_PRICE)
def handle(price):
    LOGGER.info('hash_rate called')

    with psycopg2.connect(**config['custom_data']) as connection2:
        with connection2.cursor() as c2:
            c2.execute('SELECT * FROM miners WHERE is_active is true')
            miners = c2.fetchall()

    all_data = {}
    # Opening DB. When the 'with' block ends, connection will be closed
    with psycopg2.connect(**config['blockchain_data']) as connection:
        data = CoinMetrics().get_values(start_date='2009-01-01')
        for item in data:
            if any(item[metric] is None for metric in ['difficulty', 'hash-rate', 'miners-revenue', 'market-price']):
                continue
            for metric in ['difficulty', 'hash-rate', 'miners-revenue', 'market-price']:
                if metric in item:
                    # this is because table name can't contain hyphens
                    table_name = metric.replace('-', '_')
                    value = item[metric]
                    timestamp = item['timestamp']

                    save_values([(timestamp, value)], connection, table_name)

                    if timestamp not in all_data:
                        all_data[timestamp] = {}

                    all_data[timestamp][metric] = value

        # =============================================================================
        #        # This is to create block reward time series
        #        for timestamp, data in all_data.items():
        #            # 0 <= timestamp < 28 November 2012
        #            if timestamp < 1353967200:
        #                data['block-reward'] = 50.0
        #            # 28 November 2012 <= timestamp < 9 July 2016
        #            elif timestamp < 1467925200:
        #                data['block-reward'] = 25.0
        #           # 9 July 2016 <= timestamp < 01 May 2020   # Will not work forever
        #            elif timestamp < 1588291200:
        #                data['block-reward'] = 12.5
        #            else:
        #                LOGGER.warning(f"Timestamp {timestamp} is out of range")
        #                data['block-reward'] = None
        #        save_values(((timestamp, data['block-reward']) for timestamp,
        #                        data in all_data.items()),
        #                    connection, 'block_reward')
        # =============================================================================
        #        # Profitability threshold calculation -- block rewards only (bro)
        #        for timestamp, data in all_data.items():
        #            try:
        #                data['thresh-bro'] = 1.0e+09 / (2 ** 32 * data['difficulty'])\
        #                 *data['block-reward']* data['market-price'] / price * 3.6e+06
        #            except KeyError:
        #                pass
        #            except ZeroDivisionError:
        #                data['thresh-bro'] = float('inf')
        #                LOGGER.warning(f"Zero div: timestamp={timestamp},data={data}")
        #        save_values(((timestamp, data['prof-threshold-block-reward-only'])
        #                    for timestamp, data
        #                     in all_data.items() if 'thresh-bro' in data),
        #                    connection, 'prof_threshold_block_reward_only')
        # =============================================================================

        # Profitability threshold calculation based on the miners revenue est.
        LOGGER.info(f"prof-threshold: as of {datetime.utcnow().isoformat()}")
        for timestamp, data in all_data.items():
            try:
                data['prof-threshold'] = (data['miners-revenue'] /
                                          (data['hash-rate'] * 60 * 60 * 24)) / (price / 3.6e+06) / 1000
            except KeyError:
                pass
            except ZeroDivisionError:
                data['prof-threshold'] = float('inf')
                LOGGER.warning(f"Zero div: timestamp={timestamp}, data={data}")

        save_values(((timestamp, data['prof-threshold']) for timestamp, data
                     in all_data.items() if 'prof-threshold' in data),
                    connection, 'prof_threshold')

        # Calculating energy consumption
        LOGGER.info(f"energy-consumption: as of {datetime.utcnow().isoformat()}")
        with connection.cursor() as c:
            # Creating table. timestamp is a PRIMARY KEY, values are unique
            c.execute("CREATE TABLE IF NOT EXISTS energy_consumption (timestamp "
                      "INT PRIMARY KEY, date TEXT, max_consumption REAL, min_consumption REAL, "
                      "guess_consumption REAL, all_prof_eqp TEXT, all_prof_eqp_qty TEXT);")
            # Template of the query to paste a row to a table
            insert_sql = "INSERT INTO energy_consumption (timestamp, date, max_consumption, " \
                         "min_consumption, guess_consumption, all_prof_eqp, all_prof_eqp_qty) " \
                         "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT " \
                         "energy_consumption_pkey DO NOTHING;"
            prof_eqp = []  # temp var for list of profit. eqp efficiency
            prof_eqp_all = []  # list of lists of profit. eqp efficiency
            prof_eqp_qty = []  # temp var for the list of profit. eqp qty
            prof_eqp_qty_all = []  # list of lists of profitable equipment qty
            max_all = []
            min_all = []
            guess_all = []
            ts_all = []

            data_df = pd.DataFrame.from_dict(all_data, orient='index')
            data_ma = data_df.rolling(window=14, min_periods=1).mean()

            typed_hasrates = load_typed_hasrates()  # @todo: uncomment this for S7/S9
            typed_avg_effciency = get_avg_effciency_by_miners_types_old(miners)  # @todo: uncomment this for S7/S9
            for timestamp, data in all_data.items():
                hash_rates = get_hash_rates_by_miners_types(typed_hasrates,
                                                            timestamp)  # @todo: uncomment this for S7/S9
                for miner in miners:
                    if timestamp > miner[1] and data_ma['prof-threshold'][timestamp] > miner[2]:
                        # ^^current date and date of miner release ^^checks if miner is profitable;
                        # if yes, adds miner's efficiency and qty to the lists:
                        # prof_eqp.append(miner[2]) # @todo: remove this for S7/S9
                        # prof_eqp_qty.append(miner[3]) # @todo: remove this for S7/S9
                        # @todo: uncomment this for S7/S9
                        type = miner[5]
                        if not type:
                            prof_eqp.append(miner[2])
                            prof_eqp_qty.append(miner[3])
                        # @todo: uncomment this for S7/S9
                prof_eqp_qty_all.append(prof_eqp_qty)
                prof_eqp_all.append(prof_eqp)
                try:
                    max_consumption = max(prof_eqp) * data['hash-rate'] * 365.25 * 24 / 1e9 * 1.2
                    min_consumption = min(prof_eqp) * data['hash-rate'] * 365.25 * 24 / 1e9 * 1.01
                    # @todo: remove this for S7/S9
                    # if len(prof_eqp) == 0:
                    #     guess_consumption = 0
                    # else:
                    #     guess_consumption = sum(prof_eqp) / len(prof_eqp) * data['hash-rate'] * 365.25 * 24 / 1e+9 * 1.1
                    # @todo: /remove this for S7/S9
                    guess_consumption = get_guess_consumption(prof_eqp, data['hash-rate'], hash_rates,
                                                              typed_avg_effciency)  # @todo: uncomment this for S7/S9
                # ====this=is=for=weighting===================================================
                #                 weighted_sum = 0
                #                 eqp_qty_this_day = 0
                #                 # calculating the guess_consumption using the weighted average of prof_eqp efficiencies:
                #                 for j in range(0, len(prof_eqp)):
                #                     weighted_sum = weighted_sum + prof_eqp[j]*prof_eqp_qty[j]
                #                     eqp_qty_this_day = eqp_qty_this_day + prof_eqp_qty[j]
                #                 guess_consumption = weighted_sum/eqp_qty_this_day*hash_rate[i][2]*365.25*24/1e+9*1.05
                # ===========================================================================
                except Exception as error:  # in case if mining is not profitable (impossible to find MAX of empty list)
                    LOGGER.warning(f"Mining was unprofitable at timestamp={timestamp}: '{error}'")
                    max_consumption = max_all[-1] if len(max_all) > 0 else 0
                    min_consumption = min_all[-1] if len(min_all) > 0 else 0
                    guess_consumption = guess_all[-1] if len(guess_all) > 0 else 0
                max_all.append(max_consumption)
                min_all.append(min_consumption)
                guess_all.append(guess_consumption)
                ts_all.append(timestamp)
                date = datetime.utcfromtimestamp(timestamp).isoformat()
                prof_eqp = str(prof_eqp).strip('[]')  # making str from prof_eqp
                prof_eqp_qty = str(prof_eqp_qty).strip('[]')
                try:
                    c.execute(insert_sql, (timestamp, date, max_consumption,
                                           min_consumption, guess_consumption,
                                           prof_eqp, prof_eqp_qty))
                # If the row with this timestamp already exist, ignore it:
                except Exception as error:
                    LOGGER.warning(f"Energy consumption saving error at "
                                   f"timestamp={timestamp}: '{error}'")
                    pass
                prof_eqp = []
                prof_eqp_qty = []

            # calculating MA of the resulting stats
            LOGGER.info(f"energy-consump-MA: as of {datetime.utcnow().isoformat()}")
            energy_df = pd.DataFrame(list(zip(max_all, min_all, guess_all)),
                                     index=ts_all, columns=['MAX', 'MIN', 'GUESS'])
            energy_ma = energy_df.rolling(window=7, min_periods=1).mean()

            c.execute("CREATE TABLE IF NOT EXISTS energy_consumption_ma (timestamp INT PRIMARY KEY, "
                      "date TEXT, max_consumption REAL, min_consumption REAL, guess_consumption REAL);")
            insert_sql = "INSERT INTO energy_consumption_ma (timestamp, date, max_consumption, " \
                         "min_consumption, guess_consumption) VALUES (%s, %s, %s, %s, %s) " \
                         "ON CONFLICT ON CONSTRAINT energy_consumption_ma_pkey DO NOTHING;"

            max_ma = list(energy_ma['MAX'])
            min_ma = list(energy_ma['MIN'])
            guess_ma = list(energy_ma['GUESS'])
            ts = ts_all
            date_all = []
            for t in ts:
                date = datetime.utcfromtimestamp(t).isoformat()
                date_all.append(date)

            for item in zip(ts, date_all, max_ma, min_ma, guess_ma):
                try:
                    c.execute(insert_sql, item)
                except Exception as error:
                    LOGGER.warning(f"Energy consumption MA saving err: {error}'")
                    pass


def save_values(values, connection, table_name):
    # Creating cursor to work with DB
    cursor = connection.cursor()
    # Creating table. timestamp is a PRIMARY KEY, values are unique
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name}"
                   f" (timestamp INT PRIMARY KEY, date TEXT, value REAL);")
    # Template of the query to paste a row to a table
    insert_sql = f"INSERT INTO {table_name} (timestamp, date, value) " \
                 f"VALUES (%s, %s, %s) ON CONFLICT ON CONSTRAINT {table_name}_pkey DO NOTHING;"
    # Taking 'values' from the API reply and inputting rows one by one
    for timestamp, value in values:
        # The second column is going to be date in readable format
        date = datetime.utcfromtimestamp(timestamp).isoformat()
        # Trying to insert row
        try:
            cursor.execute(insert_sql, (timestamp, date, value))
        # If the row with this timestamp already exist, ignore it
        except Exception as error:
            LOGGER.exception(f"{table_name}: {str(error)}")
            pass
        finally:
            connection.commit()
