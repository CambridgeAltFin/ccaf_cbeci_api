from config import config

import click
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime


@click.command(name='eth-pos:calc:electricity')
def handle():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor()
        cursor.execute("select date, prysm, lighthouse, lodestar, nimbus, teku from eth_pos_nodes "
                       "where source = 'prometheus'")
        data = [{
            'Date': date.strftime('%Y-%m-%d'),
            'Prysm': prysm,
            'Lighthouse': lighthouse,
            'Lodestar': lodestar,
            'Nimbus': nimbus,
            'Teku': teku,
        } for (date, prysm, lighthouse, lodestar, nimbus, teku) in cursor.fetchall()]

        consensus = pd.DataFrame.from_records(data)

        # consensus = consensus.drop(['Others', 'Grandine'], axis=1)
        cols = ['Prysm', 'Lighthouse', 'Lodestar', 'Nimbus', 'Teku']
        consensus['Total'] = consensus[cols].sum(axis=1)

        consensus['Prysm%'] = consensus['Prysm'] / consensus['Total'] * 100
        consensus['Lighthouse%'] = consensus['Lighthouse'] / consensus['Total'] * 100
        consensus['Lodestar%'] = consensus['Lodestar'] / consensus['Total'] * 100
        consensus['Nimbus%'] = consensus['Nimbus'] / consensus['Total'] * 100
        consensus['Teku%'] = consensus['Teku'] / consensus['Total'] * 100

        execution_shares = [['Geth_%exe', '84.52'], ['Erigon_%exe', '8.95'], ['Besu_%exe', '6.53']]
        exe_data = pd.DataFrame(execution_shares)
        execution = exe_data.transpose()
        execution.columns = execution.iloc[0]
        execution = execution[1:]
        execution = execution.astype(float)

        idle = [['config4_idle', 'config5_idle', 'config6_idle'], ['3.66', '25.04', '78.17']]
        idle_power = pd.DataFrame(idle)
        idle_power.columns = idle_power.iloc[0]
        idle_power = idle_power[1:]
        idle_power = idle_power.astype(float)

        node_elec_config4 = [
            ['Geth_aMEC', 'Erigon_aMEC', 'Besu_aMEC', 'Prysm_aMEC', 'Lighthouse_aMEC', 'Teku_aMEC', 'Nimbus_aMEC',
             'Lodestar_aMEC'],
            ['11.23', '18.60', '30.25', '3.51', '2.75', '3.71', '1.67', '3.14']]
        node_elec_config4 = pd.DataFrame(node_elec_config4)
        node_elec_config4.columns = node_elec_config4.iloc[0]
        node_elec_config4 = node_elec_config4[1:]
        node_elec_config4 = node_elec_config4.astype(float)

        node_elec_config5 = [
            ['Geth_aMEC', 'Erigon_aMEC', 'Besu_aMEC', 'Prysm_aMEC', 'Lighthouse_aMEC', 'Teku_aMEC', 'Nimbus_aMEC',
             'Lodestar_aMEC'],
            ['9.70', '17.59', '31.02', '2.87', '3.14', '3.22', '2.08', '3.89']]
        node_elec_config5 = pd.DataFrame(node_elec_config5)
        node_elec_config5.columns = node_elec_config5.iloc[0]
        node_elec_config5 = node_elec_config5[1:]
        node_elec_config5 = node_elec_config5.astype(float)

        node_elec_config6 = [
            ['Geth_aMEC', 'Erigon_aMEC', 'Besu_aMEC', 'Prysm_aMEC', 'Lighthouse_aMEC', 'Teku_aMEC', 'Nimbus_aMEC',
             'Lodestar_aMEC'],
            ['47.70', '44.62', '75.04', '24.33', '18.84', '27.46', '17.11', '33.55']]

        node_elec_config6 = pd.DataFrame(node_elec_config6)
        node_elec_config6.columns = node_elec_config6.iloc[0]
        node_elec_config6 = node_elec_config6[1:]
        node_elec_config6 = node_elec_config6.astype(float)

        config4_list = [consensus, idle_power, node_elec_config4, execution]
        data_config4 = pd.concat(config4_list, axis=1).ffill().bfill()

        data_config4['PG%'] = data_config4['Prysm%'] * data_config4['Geth_%exe'] / 10000
        data_config4['PE%'] = data_config4['Prysm%'] * data_config4['Erigon_%exe'] / 10000
        data_config4['PB%'] = data_config4['Prysm%'] * data_config4['Besu_%exe'] / 10000

        data_config4['LiG%'] = data_config4['Lighthouse%'] * data_config4['Geth_%exe'] / 10000
        data_config4['LiE%'] = data_config4['Lighthouse%'] * data_config4['Erigon_%exe'] / 10000
        data_config4['LiB%'] = data_config4['Lighthouse%'] * data_config4['Besu_%exe'] / 10000

        data_config4['LoG%'] = data_config4['Lodestar%'] * data_config4['Geth_%exe'] / 10000
        data_config4['LoE%'] = data_config4['Lodestar%'] * data_config4['Erigon_%exe'] / 10000
        data_config4['LoB%'] = data_config4['Lodestar%'] * data_config4['Besu_%exe'] / 10000

        data_config4['NG%'] = data_config4['Nimbus%'] * data_config4['Geth_%exe'] / 10000
        data_config4['NE%'] = data_config4['Nimbus%'] * data_config4['Erigon_%exe'] / 10000
        data_config4['NB%'] = data_config4['Nimbus%'] * data_config4['Besu_%exe'] / 10000

        data_config4['TG%'] = data_config4['Teku%'] * data_config4['Geth_%exe'] / 10000
        data_config4['TE%'] = data_config4['Teku%'] * data_config4['Erigon_%exe'] / 10000
        data_config4['TB%'] = data_config4['Teku%'] * data_config4['Besu_%exe'] / 10000

        data_config4['Lower Electricity Demand kW'] = ((data_config4['Prysm_aMEC'] + data_config4['Geth_aMEC']) *
                                                       data_config4['PG%'] +
                                                       (data_config4['Prysm_aMEC'] + data_config4['Erigon_aMEC']) *
                                                       data_config4['PE%'] +
                                                       (data_config4['Prysm_aMEC'] + data_config4['Besu_aMEC']) *
                                                       data_config4['PB%'] +
                                                       (data_config4['Lighthouse_aMEC'] + data_config4['Geth_aMEC']) *
                                                       data_config4['LiG%'] +
                                                       (data_config4['Lighthouse_aMEC'] + data_config4['Erigon_aMEC']) *
                                                       data_config4['LiE%'] +
                                                       (data_config4['Lighthouse_aMEC'] + data_config4['Besu_aMEC']) *
                                                       data_config4['LiB%'] +
                                                       (data_config4['Lodestar_aMEC'] + data_config4['Geth_aMEC']) *
                                                       data_config4['LoG%'] +
                                                       (data_config4['Lodestar_aMEC'] + data_config4['Erigon_aMEC']) *
                                                       data_config4['LoE%'] +
                                                       (data_config4['Lodestar_aMEC'] + data_config4['Besu_aMEC']) *
                                                       data_config4['LoB%'] +
                                                       (data_config4['Nimbus_aMEC'] + data_config4['Geth_aMEC']) *
                                                       data_config4['NG%'] +
                                                       (data_config4['Nimbus_aMEC'] + data_config4['Erigon_aMEC']) *
                                                       data_config4['NE%'] +
                                                       (data_config4['Nimbus_aMEC'] + data_config4['Besu_aMEC']) *
                                                       data_config4['NB%'] +
                                                       (data_config4['Teku_aMEC'] + data_config4['Geth_aMEC']) *
                                                       data_config4['TG%'] +
                                                       (data_config4['Teku_aMEC'] + data_config4['Erigon_aMEC']) *
                                                       data_config4['TE%'] +
                                                       (data_config4['Teku_aMEC'] + data_config4['Besu_aMEC']) *
                                                       data_config4['TB%'] +
                                                       data_config4['config4_idle']) * data_config4['Total'] / 1000

        data_config4['Lower Electricity Consumption kWh'] = data_config4['Lower Electricity Demand kW'] * 24 * 365.25

        lower_elec = data_config4[['Date', 'Lower Electricity Demand kW', 'Lower Electricity Consumption kWh']]

        config6_list = [consensus, idle_power, node_elec_config6, execution]
        data_config6 = pd.concat(config6_list, axis=1).ffill().bfill()

        data_config6['PG%'] = data_config6['Prysm%'] * data_config6['Geth_%exe'] / 10000
        data_config6['PE%'] = data_config6['Prysm%'] * data_config6['Erigon_%exe'] / 10000
        data_config6['PB%'] = data_config6['Prysm%'] * data_config6['Besu_%exe'] / 10000

        data_config6['LiG%'] = data_config6['Lighthouse%'] * data_config6['Geth_%exe'] / 10000
        data_config6['LiE%'] = data_config6['Lighthouse%'] * data_config6['Erigon_%exe'] / 10000
        data_config6['LiB%'] = data_config6['Lighthouse%'] * data_config6['Besu_%exe'] / 10000

        data_config6['LoG%'] = data_config6['Lodestar%'] * data_config6['Geth_%exe'] / 10000
        data_config6['LoE%'] = data_config6['Lodestar%'] * data_config6['Erigon_%exe'] / 10000
        data_config6['LoB%'] = data_config6['Lodestar%'] * data_config6['Besu_%exe'] / 10000

        data_config6['NG%'] = data_config6['Nimbus%'] * data_config6['Geth_%exe'] / 10000
        data_config6['NE%'] = data_config6['Nimbus%'] * data_config6['Erigon_%exe'] / 10000
        data_config6['NB%'] = data_config6['Nimbus%'] * data_config6['Besu_%exe'] / 10000

        data_config6['TG%'] = data_config6['Teku%'] * data_config6['Geth_%exe'] / 10000
        data_config6['TE%'] = data_config6['Teku%'] * data_config6['Erigon_%exe'] / 10000
        data_config6['TB%'] = data_config6['Teku%'] * data_config6['Besu_%exe'] / 10000

        data_config6['Upper Electricity Demand kW'] = ((data_config6['Prysm_aMEC'] + data_config6['Geth_aMEC']) *
                                                       data_config6['PG%'] +
                                                       (data_config6['Prysm_aMEC'] + data_config6['Erigon_aMEC']) *
                                                       data_config6['PE%'] +
                                                       (data_config6['Prysm_aMEC'] + data_config6['Besu_aMEC']) *
                                                       data_config6['PB%'] +
                                                       (data_config6['Lighthouse_aMEC'] + data_config6['Geth_aMEC']) *
                                                       data_config6['LiG%'] +
                                                       (data_config6['Lighthouse_aMEC'] + data_config6['Erigon_aMEC']) *
                                                       data_config6['LiE%'] +
                                                       (data_config6['Lighthouse_aMEC'] + data_config6['Besu_aMEC']) *
                                                       data_config6['LiB%'] +
                                                       (data_config6['Lodestar_aMEC'] + data_config6['Geth_aMEC']) *
                                                       data_config6['LoG%'] +
                                                       (data_config6['Lodestar_aMEC'] + data_config6['Erigon_aMEC']) *
                                                       data_config6['LoE%'] +
                                                       (data_config6['Lodestar_aMEC'] + data_config6['Besu_aMEC']) *
                                                       data_config6['LoB%'] +
                                                       (data_config6['Nimbus_aMEC'] + data_config6['Geth_aMEC']) *
                                                       data_config6['NG%'] +
                                                       (data_config6['Nimbus_aMEC'] + data_config6['Erigon_aMEC']) *
                                                       data_config6['NE%'] +
                                                       (data_config6['Nimbus_aMEC'] + data_config6['Besu_aMEC']) *
                                                       data_config6['NB%'] +
                                                       (data_config6['Teku_aMEC'] + data_config6['Geth_aMEC']) *
                                                       data_config6['TG%'] +
                                                       (data_config6['Teku_aMEC'] + data_config6['Erigon_aMEC']) *
                                                       data_config6['TE%'] +
                                                       (data_config6['Teku_aMEC'] + data_config6['Besu_aMEC']) *
                                                       data_config6['TB%'] +
                                                       data_config6['config6_idle']) * data_config6['Total'] / 1000

        data_config6['Upper Electricity Consumption kWh'] = data_config6['Upper Electricity Demand kW'] * 24 * 365.25

        upper_elec = data_config6[['Date', 'Upper Electricity Demand kW', 'Upper Electricity Consumption kWh']]

        config5_list = [consensus, idle_power, node_elec_config5, execution]
        data_config5 = pd.concat(config5_list, axis=1).ffill().bfill()

        data_config5['PG%'] = data_config5['Prysm%'] * data_config5['Geth_%exe'] / 10000
        data_config5['PE%'] = data_config5['Prysm%'] * data_config5['Erigon_%exe'] / 10000
        data_config5['PB%'] = data_config5['Prysm%'] * data_config5['Besu_%exe'] / 10000

        data_config5['LiG%'] = data_config5['Lighthouse%'] * data_config5['Geth_%exe'] / 10000
        data_config5['LiE%'] = data_config5['Lighthouse%'] * data_config5['Erigon_%exe'] / 10000
        data_config5['LiB%'] = data_config5['Lighthouse%'] * data_config5['Besu_%exe'] / 10000

        data_config5['LoG%'] = data_config5['Lodestar%'] * data_config5['Geth_%exe'] / 10000
        data_config5['LoE%'] = data_config5['Lodestar%'] * data_config5['Erigon_%exe'] / 10000
        data_config5['LoB%'] = data_config5['Lodestar%'] * data_config5['Besu_%exe'] / 10000

        data_config5['NG%'] = data_config5['Nimbus%'] * data_config5['Geth_%exe'] / 10000
        data_config5['NE%'] = data_config5['Nimbus%'] * data_config5['Erigon_%exe'] / 10000
        data_config5['NB%'] = data_config5['Nimbus%'] * data_config5['Besu_%exe'] / 10000

        data_config5['TG%'] = data_config5['Teku%'] * data_config5['Geth_%exe'] / 10000
        data_config5['TE%'] = data_config5['Teku%'] * data_config5['Erigon_%exe'] / 10000
        data_config5['TB%'] = data_config5['Teku%'] * data_config5['Besu_%exe'] / 10000

        data_config5['Config5 Electricity Demand kW'] = ((data_config5['Prysm_aMEC'] + data_config5['Geth_aMEC']) *
                                                         data_config5['PG%'] +
                                                         (data_config5['Prysm_aMEC'] + data_config5['Erigon_aMEC']) *
                                                         data_config5['PE%'] +
                                                         (data_config5['Prysm_aMEC'] + data_config5['Besu_aMEC']) *
                                                         data_config5['PB%'] +
                                                         (data_config5['Lighthouse_aMEC'] + data_config5['Geth_aMEC']) *
                                                         data_config5['LiG%'] +
                                                         (data_config5['Lighthouse_aMEC'] + data_config5['Erigon_aMEC']) *
                                                         data_config5['LiE%'] +
                                                         (data_config5['Lighthouse_aMEC'] + data_config5['Besu_aMEC']) *
                                                         data_config5['LiB%'] +
                                                         (data_config5['Lodestar_aMEC'] + data_config5['Geth_aMEC']) *
                                                         data_config5['LoG%'] +
                                                         (data_config5['Lodestar_aMEC'] + data_config5['Erigon_aMEC']) *
                                                         data_config5['LoE%'] +
                                                         (data_config5['Lodestar_aMEC'] + data_config5['Besu_aMEC']) *
                                                         data_config5['LoB%'] +
                                                         (data_config5['Nimbus_aMEC'] + data_config5['Geth_aMEC']) *
                                                         data_config5['NG%'] +
                                                         (data_config5['Nimbus_aMEC'] + data_config5['Erigon_aMEC']) *
                                                         data_config5['NE%'] +
                                                         (data_config5['Nimbus_aMEC'] + data_config5['Besu_aMEC']) *
                                                         data_config5['NB%'] +
                                                         (data_config5['Teku_aMEC'] + data_config5['Geth_aMEC']) *
                                                         data_config5['TG%'] +
                                                         (data_config5['Teku_aMEC'] + data_config5['Erigon_aMEC']) *
                                                         data_config5['TE%'] +
                                                         (data_config5['Teku_aMEC'] + data_config5['Besu_aMEC']) *
                                                         data_config5['TB%'] +
                                                         data_config5['config5_idle']) * data_config5['Total'] / 1000

        data_config5['Config5 Electricity Consumption kWh'] = data_config5['Config5 Electricity Demand kW'] * 24 * 365.25

        config5_elec = data_config5[['Date', 'Config5 Electricity Demand kW', 'Config5 Electricity Consumption kWh']]

        lists = [upper_elec, lower_elec, config5_elec]

        best_guess = pd.concat(lists, axis=1)

        best_guess = best_guess.loc[:, ~best_guess.columns.duplicated()]

        best_guess['Best Electricity Demand kW'] = best_guess['Upper Electricity Demand kW'] * 0.25 + best_guess[
            'Lower Electricity Demand kW'] * 0.25 + best_guess['Config5 Electricity Demand kW'] * 0.5

        best_guess['Best Electricity Consumption kWh'] = best_guess['Upper Electricity Consumption kWh'] * 0.25 + \
                                                         best_guess['Lower Electricity Consumption kWh'] * 0.25 + \
                                                         best_guess['Config5 Electricity Consumption kWh'] * 0.5

        best_elec = best_guess[['Date', 'Best Electricity Demand kW', 'Best Electricity Consumption kWh']]

        graph_data = pd.concat([upper_elec, lower_elec, best_elec], axis=1)
        graph_data = graph_data.loc[:, ~graph_data.columns.duplicated()].copy()
        graph_data['ts'] = graph_data['Date'].apply(lambda x: int(datetime.strptime(x, '%Y-%m-%d').timestamp()))
        graph_data = graph_data.sort_values(by='ts').set_index('ts').rolling(window=7, min_periods=1).mean()

        cursor.execute("delete from consumptions where asset = 'eth_pos'")

        psycopg2.extras.execute_values(
            cursor,
            'insert into consumptions '
            '(asset, min_power, guess_power, max_power, min_consumption, guess_consumption, max_consumption, timestamp, date, version) '
            'values %s on conflict (asset, price, timestamp, version) do nothing',
            [(
                'eth_pos',
                x['Lower Electricity Demand kW'],
                x['Best Electricity Demand kW'],
                x['Upper Electricity Demand kW'],
                x['Lower Electricity Consumption kWh'],
                x['Best Electricity Consumption kWh'],
                x['Upper Electricity Consumption kWh'],
                ts,
                datetime.fromtimestamp(ts).strftime('%Y-%m-%d'),
                '1.2.0',
            ) for ts, x in graph_data.iterrows()]
        )
