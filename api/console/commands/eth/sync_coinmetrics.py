import click
import requests
import pandas as pd
from config import config
import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from datetime import datetime
import numpy as np
import calendar


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

dags = [
    {'Size': '2GB', 'Block': '3,840,000', 'Date': '6/8/2017'},
    {'Size': '3GB', 'Block': '7,680,000', 'Date': '5/2/2019'},
    {'Size': '4GB', 'Block': '11,520,000', 'Date': '12/25/2020'},
    {'Size': '5GB', 'Block': '15,360,000', 'Date': '08/17/2022'},
    {'Size': '6GB', 'Block': '19,200,000', 'Date': ''},
    {'Size': '8GB', 'Block': '26,880,000', 'Date': ''},
]


class EnergyConsumptionCalculator:
    def __init__(self, asset_name, miner_rev, hashrate, miner, dag, elec_cost):
        self.asset_name = asset_name
        self.miner_rev = miner_rev
        self.hashrate = hashrate
        # elf.typed_hashrate = typed_hashrate
        # elf.typed_hashrate['date'] = pd.to_datetime(self.typed_hashrate['date'])
        self.miner = miner
        self.dag = dag
        self.elec_cost = elec_cost
        self.profthreshold = None
        self.elec_consumption = None
        self.power_demand_df = None
        self.PUE_max = 1.2
        self.PUE_guess = 1.1
        self.PUE_min = 1.01

    def get_profthreshold(self, ma=True):

        self.hashrate['#Th per day'] = self.hashrate.iloc[:, 2] * 3600 * 24
        self.hashrate['timestamp'] = pd.to_datetime(self.hashrate.iloc[:, 1])
        self.miner_rev['timestamp'] = pd.to_datetime(self.miner_rev.iloc[:, 1])
        self.miner_rev['miner_rev'] = self.miner_rev.iloc[:, 6]
        profthreshold = pd.merge(self.hashrate, self.miner_rev, how='inner', on='timestamp')
        profthreshold = profthreshold[['timestamp', 'miner_rev', '#Th per day']]
        profthreshold['rev per Th per day'] = profthreshold['miner_rev'] / profthreshold['#Th per day']
        profthreshold['profthreshold'] = (self.elec_cost / profthreshold['rev per Th per day']) / (
                3.6 * 1000)  # covert from Th/Kwh to Gh/J

        if ma == True:
            # 14 days moving average
            profthreshold['profthreshold'] = profthreshold['profthreshold'].rolling(14).mean()
            profthreshold.dropna(inplace=True)
            profthreshold = profthreshold.reset_index(drop=True)
        self.profthreshold = profthreshold

        return profthreshold

    def get_profitable_equipment(self, retirement_year=5):
        self.miner['Date of release'] = pd.to_datetime(self.miner['released_at'])
        self.miner['index'] = self.miner.index
        self.profthreshold['profitable_machines'] = None
        for index, row in self.profthreshold.iterrows():
            profitable_machines = self.miner[(self.miner['Date of release'] < row['timestamp']) & (
                    self.miner['efficiency_gh_j'] > row['profthreshold'])]

            # additional constraint -- use only selected manufacturers
            # profitable_machines = profitable_machines[profitable_machines['selected_miners'] == 'Manufacturer']

            # additional constraint -- dag file size

            if row['timestamp'] < self.dag['Date'].min():
                dag_size = 0
            else:
                dag_size = self.dag[self.dag['Date'] <= row['timestamp']]['Memory size in GB'].max()
            profitable_machines = profitable_machines[
                (profitable_machines['memory']) > dag_size]  # should be more than dag size

            # additional constraint -- retire after 5 years
            profitable_machines['time difference'] = row['timestamp'] - profitable_machines['Date of release']
            profitable_machines = profitable_machines[
                profitable_machines['time difference'] <= pd.Timedelta('{} days'.format(retirement_year * 365.25))]

            profitable_machines_index = profitable_machines['index'].to_list()
            profitable_machines_index

            self.profthreshold.at[
                index, 'profitable_machines'] = profitable_machines_index if profitable_machines_index != [] else np.nan

        # if ever there is a day no machine is profitable
        self.profthreshold['profitable_machines'] = self.profthreshold['profitable_machines'].ffill(axis=0)
        self.profthreshold.dropna(inplace=True)  # if na in the beginning
        self.profthreshold = self.profthreshold.reset_index(drop=True)
        self.profthreshold['length'] = [len(x) for x in self.profthreshold['profitable_machines']]

        return self.profthreshold

    def calculate_elec_consumption(self, type='guess', ma_consumption=True):  # type can be max or min or guess
        self.profthreshold['#Gh per day'] = self.profthreshold['#Th per day'] * 1000
        elec_consumption = self.profthreshold[['timestamp', 'profitable_machines', '#Gh per day']]
        self.elec_consumption = elec_consumption
        self.elec_consumption['machine_efficiency'] = None
        if type == 'max':
            self.elec_consumption['max elec consumption'] = None
            for index, row in self.elec_consumption.iterrows():
                machine_efficiency = min(self.miner.loc[row['profitable_machines'], 'efficiency_gh_j'])
                max_annual_elec = row['#Gh per day'] / machine_efficiency * 365.25 * self.PUE_max / (3.6 * 10 ** (15))
                self.elec_consumption.at[index, 'max elec consumption'] = max_annual_elec
                self.elec_consumption.at[index, 'machine_efficiency'] = machine_efficiency

        elif type == 'min':
            self.elec_consumption['min elec consumption'] = None
            for index, row in self.elec_consumption.iterrows():
                machine_efficiency = max(self.miner.loc[row['profitable_machines'], 'efficiency_gh_j'])
                min_annual_elec = row['#Gh per day'] / machine_efficiency * 365.25 * self.PUE_min / (3.6 * 10 ** (15))
                self.elec_consumption.at[index, 'min elec consumption'] = min_annual_elec
                self.elec_consumption.at[index, 'machine_efficiency'] = machine_efficiency

        elif type == 'guess':
            self.elec_consumption['guess elec consumption'] = None
            for index, row in self.elec_consumption.iterrows():
                # select all profitable machines
                all_profitable_machines = self.miner.loc[row['profitable_machines'], :]

                machine_efficiency = sum(self.miner.loc[row['profitable_machines'], 'efficiency_gh_j']) / len(
                    self.miner.loc[row['profitable_machines']])
                guess_annual_elec = row['#Gh per day'] / machine_efficiency * 365.25 * self.PUE_guess / (
                        3.6 * 10 ** (15))
                self.elec_consumption.at[index, 'guess elec consumption'] = guess_annual_elec
                self.elec_consumption.at[index, 'machine_efficiency'] = machine_efficiency

        if ma_consumption == True:
            # 7 days moving average
            if 'max elec consumption' in self.elec_consumption.columns:
                self.elec_consumption['max elec consumption'] = self.elec_consumption['max elec consumption'].rolling(
                    7).mean()
            elif 'min elec consumption' in self.elec_consumption.columns:
                self.elec_consumption['min elec consumption'] = self.elec_consumption['min elec consumption'].rolling(
                    7).mean()
            elif 'guess elec consumption' in self.elec_consumption.columns:
                self.elec_consumption['guess elec consumption'] = self.elec_consumption[
                    'guess elec consumption'].rolling(7).mean()

            self.elec_consumption.dropna(inplace=True)
            self.elec_consumption = self.elec_consumption.reset_index(drop=True)

        return self.elec_consumption

    def calculate_power(self, type='guess', ma_consumption=True):  # type can be max or min or guess
        self.profthreshold['#Gh per day'] = self.profthreshold['#Th per day'] * 1000
        elec_power = self.profthreshold[['timestamp', 'profitable_machines', '#Gh per day']]
        self.elec_power = elec_power
        if type == 'max':
            self.elec_power = self.calculate_elec_consumption(
                type='max', ma_consumption=ma_consumption)  # unit GW (60*60*24*365.25)/ (3.6 * 10^15) * 10^9 = 8.766
            self.elec_power['max elec power'] = self.elec_power['max elec consumption'] / 8.766
        #             self.elec_power = self.calculate_elec_consumption(type = 'max') #unit GW (60*60*24*365.25)/ (3.6 * 10^15) * 10^9 = 8.766
        #             self.elec_power['max elec power'] = self.elec_power['max elec consumption'] / 8.766

        elif type == 'min':
            self.elec_power = self.calculate_elec_consumption(
                type='min', ma_consumption=ma_consumption)  # unit GW (60*60*24*365.25)/ (3.6 * 10^15) * 10^9 = 8.766
            self.elec_power['min elec power'] = self.elec_power['min elec consumption'] / 8.766

        elif type == 'guess':
            self.elec_power = self.calculate_elec_consumption(
                type='guess', ma_consumption=ma_consumption)  # unit GW (60*60*24*365.25)/ (3.6 * 10^15) * 10^9 = 8.766
            self.elec_power['guess elec power'] = self.elec_power['guess elec consumption'] / 8.766

        return self.elec_consumption

    def calculate_power_demand(self, ma=False, ma_consumption=True):
        self.get_profthreshold(ma=ma)
        self.get_profitable_equipment()
        self.power_demand_df = self.calculate_power(type='min', ma_consumption=ma_consumption)
        merge_df = self.calculate_power(type='guess', ma_consumption=ma_consumption)
        merge_df.drop('profitable_machines', axis=1, inplace=True)
        self.power_demand_df = pd.merge(self.power_demand_df, merge_df, how='left', on=['timestamp', '#Gh per day'])
        merge_df = self.calculate_power(type='max', ma_consumption=ma_consumption)
        merge_df.drop('profitable_machines', axis=1, inplace=True)
        self.power_demand_df = pd.merge(self.power_demand_df, merge_df, how='left', on=['timestamp', '#Gh per day'])
        return self.power_demand_df


@click.command(name='eth:sync:coinmetrics')
def handle():
    url = 'https://charts.coinmetrics.io/pro-api/v4/timeseries/asset-metrics?page_size=10000&assets=eth&api_key={api_key}&metrics=FeeTotUSD,BlkUncRwdUSD,IssTotUSD,FeePrioTotUSD'.format(
        api_key=config['api.coinmetrics.io']['api_key'])
    response = requests.get(url)
    json = response.json()
    miner_rev = pd.DataFrame.from_records(json['data'])
    miner_rev['BlkUncRwdUSD'] = miner_rev['BlkUncRwdUSD'].astype(float).fillna(0.0)
    miner_rev['FeePrioTotUSD'] = miner_rev['FeePrioTotUSD'].astype(float).fillna(0.0)
    miner_rev['FeeTotUSD'] = miner_rev['FeeTotUSD'].astype(float).fillna(0.0)
    miner_rev['IssTotUSD'] = miner_rev['IssTotUSD'].astype(float).fillna(0.0)
    miner_rev['time'] = pd.to_datetime(miner_rev['time']).dt.strftime('%Y-%m-%d')

    for date in miner_rev['time']:
        if date >= '2021-08-15':
            miner_rev['RevUSD'] = miner_rev['IssTotUSD'] + miner_rev['FeePrioTotUSD'] + miner_rev['BlkUncRwdUSD']
        else:
            miner_rev['RevUSD'] = miner_rev['IssTotUSD'] + miner_rev['FeeTotUSD'] + miner_rev['BlkUncRwdUSD']

    url = 'https://charts.coinmetrics.io/pro-api/v4/timeseries/asset-metrics?page_size=10000&assets=eth&api_key={api_key}&metrics=HashRate'.format(
        api_key=config['api.coinmetrics.io']['api_key'])
    response = requests.get(url)
    json = response.json()

    hashrate = pd.DataFrame.from_records(json['data'])
    hashrate['HashRate'] = hashrate['HashRate'].astype(float)

    hashrate['time'] = pd.to_datetime(hashrate['time']).dt.strftime('%Y-%m-%d')

    dag = pd.DataFrame.from_records(dags)
    dag['Date'] = pd.to_datetime(dag['Date'])
    dag['Memory size in GB'] = dag['Size'].str.extract('(\d+)GB').astype('int')
    dag = dag[pd.notnull(dag['Date'])]

    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute('select * from cbsi_miners')
        miners = pd.DataFrame.from_records(cursor.fetchall())

        miners['Date'] = pd.to_datetime(miners['released_at'])
        valid_asic = ["Bitmain", "Whatsminer", "Canaan", "Innosilicon", " Linzhi"]
        miners = miners.loc[~(miners['type'] == 'asic') | miners['brand'].isin(valid_asic)]
        miners['efficiency_gh_j'] = (miners['hashrate'] / miners['power']) / 1000
        miners['memory'] = miners['memory'].apply(lambda x: float(x))

        for cents in range(1, 21):
            elec_cost = cents / 100
            calculator = EnergyConsumptionCalculator('etc', miner_rev, hashrate, miners, dag, elec_cost)

            calculator.get_profthreshold(ma=True)
            calculator.get_profitable_equipment(retirement_year=99)
            # consumption = calculator.calculate_elec_consumption(type = 'guess')
            calculator.calculate_power(type='min', ma_consumption=True)
            calculator.calculate_power(type='max', ma_consumption=True)
            best_guess = calculator.calculate_power(type='guess', ma_consumption=True)
            best_guess['ts'] = best_guess.timestamp.values.astype(np.int64) // 10 ** 9
            best_guess.set_index('ts', inplace=True)

            graph_data = calculator.calculate_power_demand(ma=True, ma_consumption=True)

            graph_data['min elec consumption daily'] = graph_data['min elec consumption'] / 365.25
            graph_data['max elec consumption daily'] = graph_data['max elec consumption'] / 365.25
            graph_data['guess elec consumption daily'] = graph_data['guess elec consumption'] / 365.25
            graph_data['ts'] = graph_data.timestamp.values.astype(np.int64) // 10 ** 9

            insert = map(lambda x: (
                'eth',
                int(elec_cost * 100),
                x['min elec power'],
                x['guess elec power'],
                x['max elec power'],
                x['min elec consumption'],
                x['guess elec consumption'],
                x['max elec consumption'],
                x['ts'],
                datetime.fromtimestamp(x['ts']).strftime('%Y-%m-%d'),
                '1.2.0',
                best_guess.loc[x['ts'], 'machine_efficiency']
            ), graph_data.to_records())

            psycopg2.extras.execute_values(
                cursor,
                'insert into consumptions '
                '(asset, price, min_power, guess_power, max_power, min_consumption, guess_consumption, max_consumption, timestamp, date, version, machine_efficiency) '
                'values %s on conflict (asset, price, timestamp, version) do nothing',
                list(insert)
            )

            graph_data['year'] = graph_data['timestamp'].dt.year
            graph_data['month'] = graph_data['timestamp'].dt.month

            monthly_consumption = graph_data.groupby(['year', 'month'], as_index=False).agg(
                {'min elec consumption daily': sum, 'max elec consumption daily': sum,
                 'guess elec consumption daily': sum})
            rename_dict = {'min elec consumption daily': 'min elec consumption monthly',
                           'max elec consumption daily': 'max elec consumption monthly',
                           'guess elec consumption daily': 'guess elec consumption monthly'}
            monthly_consumption.rename(rename_dict, axis=1, inplace=True)
            monthly_consumption['cumulative guess consumption'] = monthly_consumption[
                'guess elec consumption monthly'].cumsum()

            insert = map(lambda x: (
                'eth',
                int(elec_cost * 100),
                x['guess elec consumption monthly'],
                x['cumulative guess consumption'],
                datetime.strptime(
                    str(x['year']) + '-' + str(x['month']) + '-' + str(calendar.monthrange(x['year'], x['month'])[1]),
                    '%Y-%m-%d').timestamp(),
                str(x['year']) + '-' + str(x['month']) + '-' + str(calendar.monthrange(x['year'], x['month'])[1]),
                '1.2.0'
            ), monthly_consumption.to_records())

            psycopg2.extras.execute_values(
                cursor,
                'insert into total_consumptions '
                '(asset, price, guess_consumption, cumulative_guess_consumption, timestamp, date, version) '
                'values %s on conflict (asset, price, timestamp, version) do nothing',
                list(insert)
            )
        return
        miners['Eff. Mh/s/W'] = miners['hashrate'] / miners['power']
        miners['available machine inclu dag'] = None
        storage_dict = {}
        for index, row in miners.iterrows():
            available_miner = miners[miners['Date'] <= row['Date']]

            if row['Date'] < dag['Date'].min():
                dag_size = 0
            else:
                dag_size = dag[dag['Date'] <= row['Date']]['Memory size in GB'].max()

            available_miner = available_miner[available_miner['memory'] > dag_size].index.to_list()
            storage_dict[row['Date']] = available_miner

        avg_machine_efficiency = []

        for key, value in storage_dict.items():
            avg_efficiency = miners.loc[value, 'Eff. Mh/s/W'].mean()
            tempt = {}
            tempt['Date'] = key
            tempt['Eff. Incl DAG file'] = avg_efficiency
            tempt['miners'] = [[x.name, str(x['Eff. Mh/s/W'])] for x in miners.loc[value].to_records()]
            avg_machine_efficiency.append(tempt)

        efficiency_graph = pd.DataFrame(avg_machine_efficiency)
        efficiency_graph['ts'] = efficiency_graph.Date.values.astype(np.int64) // 10 ** 9
        insert = map(lambda x: (
            'eth',
            x['Eff. Incl DAG file'],
            x['miners'],
            x['ts'],
            datetime.fromtimestamp(x['ts']).strftime('%Y-%m-%d'),
        ), efficiency_graph.to_records())
        psycopg2.extras.execute_values(
            cursor,
            'insert into average_machine_efficiencies '
            '(asset, value, miners, timestamp, date) '
            'values %s on conflict (asset, timestamp) do nothing',
            list(insert)
        )
