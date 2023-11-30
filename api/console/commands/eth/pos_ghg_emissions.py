import os
from datetime import datetime, timedelta, timezone

import click
import pandas as pd
import psycopg2
import psycopg2.extras

from api.digiconomist import Digiconomist
from components.eth import EthPosFactory
from components.eth.pos.dto.download import NetworkPowerDemandDto
from config import config

# Total Lifecycle Emissions in gCO2eq/kWh - updated version
co2_g_p_kwh_hydro = 21
co2_g_p_kwh_wind = 13
co2_g_p_kwh_nuc = 13
co2_g_p_kwh_solar = 35.5
co2_g_p_kwh_gas = 486  # Natural Gas
co2_g_p_kwh_coal = 1001
co2_g_p_kwh_oil = 840
co2_g_p_kwh_oRenew = 32.3

# the electricity mix for the following country will be treated as world general
manual_adjust_country_list = ['Gibraltar', 'Niue', 'Saint Helena', 'Western Sahara',
                              'Bermuda', 'Andorra', 'Curacao',
                              'Macau SAR', 'Liechtenstein']

map_start_date = '2022-02-07'

power_sources_order = {
    'Other renewable': 1,
    'Solar': 2,
    'Wind': 3,
    'Hydro': 4,
    'Nuclear': 5,
    'Oil': 6,
    'Gas': 7,
    'Coal': 8,
}


def hashrate_filter(country_name):
    """
    This function takes filters for country
    that will be treated as world general

    Input:
    country_name = str

    Output:
    Boolean
    """
    if country_name in manual_adjust_country_list:
        return 'World'
    else:
        return country_name


@click.command(name='eth-pos:ghg-emissions')
def handle():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        def save_intensity(records, name):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into co2_coefficients '
                    '(asset, name, timestamp, date, co2_coef) '
                    'values %s on conflict (asset, date) do nothing',
                    list([(
                        'eth_pos',
                        name,
                        int(datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S").replace(
                            hour=0, minute=0, tzinfo=timezone.utc).timestamp()),
                        x['timestamp'][:10],
                        x['intensity'],
                    ) for x in records])
                )

        def save_ghg_emissions(records, name, value_key):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into greenhouse_gas_emissions '
                    '(asset, price, name, timestamp, date, value) '
                    'values %s on conflict (asset, price, date, name) do nothing',
                    list([(
                        'eth_pos',
                        '0',
                        name,
                        int(datetime.strptime(x['timestamp'], "%Y-%m-%dT%H:%M:%S").replace(
                            hour=0, minute=0, tzinfo=timezone.utc).timestamp()),
                        x['timestamp'][:10],
                        x[value_key],
                    ) for x in records])
                )

        def save_cumulative_ghg_emissions(records):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into cumulative_greenhouse_gas_emissions '
                    '(asset, price, timestamp, date, value, cumulative_value) '
                    'values %s on conflict (asset, price, date) do nothing',
                    list([(
                        'eth_pos',
                        '0',
                        x['timestamp'],
                        datetime.fromtimestamp(x['timestamp']).strftime("%Y-%m-%d"),
                        x['Monthly consumption GWh_CO2'],
                        x['Cumulative consumption GWh_CO2'],
                    ) for x in records])
                )

        def save_power_sources(records, power_type):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into power_sources '
                    '(asset, type, name, timestamp, date, value, "order") '
                    'values %s on conflict (asset, type, name, date) do nothing',
                    list([(
                        'eth_pos',
                        power_type,
                        x['Power Source'],
                        int(x['Date'].timestamp()),
                        x['Date'].strftime("%Y-%m-%d"),
                        x['Power Source Share'],
                        power_sources_order[x['Power Source']]
                    ) for x in records])
                )

        save_digiconomist_live_data(cursor)

        cursor.execute("select * from eth_pos_nodes_distribution "
                       "where source = 'monitoreth' order by date")
        eth_node_share = pd.DataFrame(cursor.fetchall())

        cursor.execute("select country, code, id as country_id, code3 from countries")
        countries = pd.DataFrame(cursor.fetchall())

        eth_country_node_share = pd.merge(eth_node_share, countries, on='country_id')

        country_map = pd.read_csv(os.getcwd() + '/../storage/eth_pos/country_map_file.csv')
        eth_country_node_share = pd.merge(eth_country_node_share, country_map, how='left', left_on='code3',
                                          right_on='Code')

        # for unmatched entities - use World
        eth_country_node_share['Entity'] = eth_country_node_share['Entity'].fillna('World')
        eth_country_node_share['Code'] = eth_country_node_share['Code'].fillna('OWID_WRL')

        # manual adjust for selected countries
        eth_country_node_share['Country Equivalent'] = eth_country_node_share['Entity'].apply(
            lambda x: hashrate_filter(x))
        eth_country_node_share['date'] = pd.to_datetime(eth_country_node_share['date'])
        eth_country_node_share['year'] = eth_country_node_share.date.dt.year
        eth_country_node_share['month'] = eth_country_node_share.date.dt.month
        eth_country_node_share['day'] = eth_country_node_share.date.dt.day
        eth_country_node_share['daily_total_nodes'] = eth_country_node_share.groupby(['year', 'month', 'day'])[
            'number_of_nodes'].transform('sum')
        eth_country_node_share['share of global nodes'] = eth_country_node_share['number_of_nodes'] / \
                                                          eth_country_node_share['daily_total_nodes']
        eth_country_node_share = eth_country_node_share[
            ['date', 'country', 'share of global nodes', 'number_of_nodes', 'Code', 'Country Equivalent']]

        df2 = pd.DataFrame(
            [NetworkPowerDemandDto(x) for x in EthPosFactory.create_repository().get_network_power_demand()]
        )
        df2['date'] = pd.to_datetime(df2['timestamp'])
        historical_elec = df2[df2['date'] < map_start_date]
        df2 = df2[(df2['date'] >= map_start_date)]

        df2.drop('date', axis=1, inplace=True)
        df2 = df2.reset_index(drop=True)

        df3 = pd.read_csv(os.getcwd() + '/../storage/eth_pos/share-elec-by-source.csv')
        df3.rename(columns={'Coal (% electricity)': 'Share of Coal', 'Gas (% electricity)': 'Share of Gas',
                            'Hydro (% electricity)': 'Share of Hydro', 'Solar (% electricity)': 'Share of Solar',
                            'Wind (% electricity)': 'Share of Wind', 'Oil (% electricity)': 'Share of Oil',
                            'Nuclear (% electricity)': 'Share of Nuclear',
                            'Other renewables excluding bioenergy (% electricity)': 'Share of Other renewable'},
                   inplace=True)
        df3['Share of Coal'] = df3['Share of Coal'] / 100
        df3['Share of Gas'] = df3['Share of Gas'] / 100
        df3['Share of Hydro'] = df3['Share of Hydro'] / 100
        df3['Share of Oil'] = df3['Share of Oil'] / 100
        df3['Share of Wind'] = df3['Share of Wind'] / 100
        df3['Share of Nuclear'] = df3['Share of Nuclear'] / 100
        df3['Share of Solar'] = df3['Share of Solar'] / 100
        df3['Share of Other renewable'] = (df3['Share of Other renewable'] + df3['Bioenergy (% electricity)']) / 100
        df3.drop('Bioenergy (% electricity)', axis=1, inplace=True)
        df3 = df3.fillna(0)

        ele_cum = pd.DataFrame(EthPosFactory.create_repository().get_monthly_total_electricity_consumption())
        ele_cum['Month'] = ele_cum['timestamp'].apply(lambda x: datetime.fromtimestamp(x))

        his_energy_mix = df3[df3['Entity'] == 'World'].reset_index()
        his_energy_mix['CO2_Coef'] = (his_energy_mix['Share of Coal'] * co2_g_p_kwh_coal +
                                      his_energy_mix['Share of Gas'] * co2_g_p_kwh_gas +
                                      his_energy_mix['Share of Oil'] * co2_g_p_kwh_oil +
                                      his_energy_mix['Share of Nuclear'] * co2_g_p_kwh_nuc +
                                      his_energy_mix['Share of Hydro'] * co2_g_p_kwh_hydro +
                                      his_energy_mix['Share of Wind'] * co2_g_p_kwh_wind +
                                      his_energy_mix['Share of Solar'] * co2_g_p_kwh_solar +
                                      his_energy_mix['Share of Other renewable'] * co2_g_p_kwh_oRenew)
        his_co2 = his_energy_mix[['Year', 'CO2_Coef']]

        historical_elec['Date and Time'] = pd.to_datetime(historical_elec['date'])
        historical_elec['Year'] = historical_elec['date'].dt.year

        # Calculate Global historical CO2 emissions
        his_graph_data = pd.merge(historical_elec, his_co2, how='left', on='Year')
        his_graph_data.rename({'CO2_Coef': 'intensity'}, axis=1, inplace=True)
        his_graph_data['MAX'] = his_graph_data['max_consumption'].astype(float)
        his_graph_data['MIN'] = his_graph_data['min_consumption'].astype(float)
        his_graph_data['GUESS'] = his_graph_data['guess_consumption'].astype(float)

        # Unit change: use tons
        his_graph_data['MAX_CO2'] = (his_graph_data['GUESS'] * 10 ** 6 * co2_g_p_kwh_coal) / 10 ** 6
        # 10**6 = conversion to tons #10**6 # conversion from gwh to kwh
        his_graph_data['MIN_CO2'] = (his_graph_data['GUESS'] * 10 ** 6 * co2_g_p_kwh_hydro) / 10 ** 6
        # 10**6 = conversion to tons #10**6 # conversion from gwh to kwh
        his_graph_data['GUESS_CO2'] = (his_graph_data['GUESS'] * 10 ** 6 * his_graph_data['intensity']) / 10 ** 6
        # 10**6 = conversion to tons #10**6 # conversion from gwh to kwh

        df3['CO2_Coef'] = (df3['Share of Coal'] * co2_g_p_kwh_coal +
                           df3['Share of Gas'] * co2_g_p_kwh_gas +
                           df3['Share of Oil'] * co2_g_p_kwh_oil +
                           df3['Share of Nuclear'] * co2_g_p_kwh_nuc +
                           df3['Share of Hydro'] * co2_g_p_kwh_hydro +
                           df3['Share of Wind'] * co2_g_p_kwh_wind +
                           df3['Share of Solar'] * co2_g_p_kwh_solar +
                           df3['Share of Other renewable'] * co2_g_p_kwh_oRenew)

        eth_country_node_share['Year'] = eth_country_node_share['date'].dt.year
        df3_to_merge = df3[['Entity', 'Year', 'CO2_Coef']]
        populate_until_year = datetime.now().year
        df3_to_merge = pd.DataFrame(populate_co2(df3_to_merge, 'Entity', 'Year', populate_until_year))
        eth_country_node_share = pd.merge(eth_country_node_share, df3_to_merge, how='left',
                                          left_on=['Year', 'Country Equivalent'],
                                          right_on=['Year', 'Entity'])

        df2['date'] = pd.to_datetime(df2['timestamp'])
        eth_country_node_share['date'] = pd.to_datetime(eth_country_node_share['date'].dt.date)

        df_country = pd.merge(eth_country_node_share, df2, how='left', on='date')
        df_country['MAX'] = df_country['max_consumption'].astype(float)
        df_country['MIN'] = df_country['min_consumption'].astype(float)
        df_country['GUESS'] = df_country['guess_consumption'].astype(float)
        df_country['MAX_CO2'] = (df_country['share of global nodes'] * df_country['MAX'] * 10 ** 6 * df_country[
            'CO2_Coef']) / 10 ** 6
        df_country['MIN_CO2'] = (df_country['share of global nodes'] * df_country['MIN'] * 10 ** 6 * df_country[
            'CO2_Coef']) / 10 ** 6
        df_country['GUESS_CO2'] = (df_country['share of global nodes'] * df_country['GUESS'] * 10 ** 6 * df_country[
            'CO2_Coef']) / 10 ** 6

        df4 = df_country.groupby('date').agg({'MAX_CO2': 'sum', 'MIN_CO2': 'sum', 'GUESS_CO2': 'sum',
                                              'MAX': 'first', 'MIN': 'first', 'GUESS': 'first'})
        df4['date'] = df4.index
        df4['timestamp'] = df4['date'].apply(
            lambda x: x.strftime("%Y-%m-%dT00:00:00")
        )
        df4.index.names = ['Date and Time']
        df4['intensity'] = df4['GUESS_CO2'] / df4['GUESS']

        save_intensity(his_graph_data.to_dict('records'), 'Historical')
        save_intensity(df4.to_dict('records'), 'Assessed')

        df4['MAX_CO2'] = (df4['GUESS'] * 10 ** 6 * co2_g_p_kwh_coal) / 10 ** 6
        # 10**6 = conversion to tons and 10**6 # conversion from gwh to kwh
        df4['MIN_CO2'] = (df4['GUESS'] * 10 ** 6 * co2_g_p_kwh_hydro) / 10 ** 6
        # 10**6 = conversion to tons and 10**6 # conversion from gwh to kwh
        df4['GUESS_CO2'] = (df4['GUESS'] * 10 ** 6 * df4['intensity']) / 10 ** 6
        # 10**6 = conversion to tons and 10**6 # conversion from gwh to kwh

        save_ghg_emissions(his_graph_data.to_dict('records'), 'Historical Hydro-only', 'MIN_CO2')
        save_ghg_emissions(his_graph_data.to_dict('records'), 'Historical Coal-only', 'MAX_CO2')
        save_ghg_emissions(his_graph_data.to_dict('records'), 'Historical Estimated', 'GUESS_CO2')

        save_ghg_emissions(df4.to_dict('records'), 'Assessed Hydro-only', 'MIN_CO2')
        save_ghg_emissions(df4.to_dict('records'), 'Assessed Coal-only', 'MAX_CO2')
        save_ghg_emissions(df4.to_dict('records'), 'Assessed Estimated', 'GUESS_CO2')

        # 1. append historical co2 coefficent, map calculated co2 coefficient and future extrapolated co2 coefficient together
        elec_df_list = [his_graph_data, df4]
        co2_coefficient_list = []
        for data in elec_df_list:
            co2_coefficient_list.append(data)
        co2_coefficient_total_df = pd.concat(co2_coefficient_list)

        # 2. get co2 coefficient for each month
        co2_coefficient_total_df['Date and Time'] = pd.to_datetime(co2_coefficient_total_df['timestamp'])
        co2_coefficient_total_df['Year'] = co2_coefficient_total_df['Date and Time'].dt.year
        co2_coefficient_total_df['Month'] = co2_coefficient_total_df['Date and Time'].dt.month
        co2_coefficient_total_df = co2_coefficient_total_df.groupby(['Year', 'Month'], as_index=False)[
            'intensity'].max()
        co2_coefficient_total_df['Year'] = co2_coefficient_total_df['Year'].apply(lambda x: int(x))
        co2_coefficient_total_df['Month'] = co2_coefficient_total_df['Month'].apply(lambda x: int(x))
        co2_coefficient_total_df['Date and Time'] = pd.to_datetime(
            co2_coefficient_total_df['Year'].astype('str') + '-' +
            co2_coefficient_total_df['Month'].astype('str') + '-01')

        # 3. drop redundant column and reorder the dataframe
        co2_coefficient_total_df.drop(['Year', 'Month'], axis=1, inplace=True)
        co2_coefficient_total_df = co2_coefficient_total_df[['Date and Time', 'intensity']]

        ele_cum['Date'] = pd.to_datetime(ele_cum['Month'])
        ele_cum['Date'] = ele_cum['Date'].dt.date
        co2_coefficient_total_df['Date and Time'] = pd.to_datetime(co2_coefficient_total_df['Date and Time'])

        # Convert 'Date' column in ele_cum to datetime
        ele_cum['Date'] = pd.to_datetime(ele_cum['Date'])

        # Now, perform the merge using the 'Date' column in both DataFrames
        ele_cum_with_co2 = pd.merge(ele_cum, co2_coefficient_total_df, how='left', left_on='Date',
                                    right_on='Date and Time')
        # eleCum_with_co2['Monthly consumption GWh_CO2'] = (eleCum_with_co2['Monthly consumption, GWh'] * 10**6 * eleCum_with_co2['intensity'])/ 1000000000000
        # 10**12 = conversion to million tons and 10**6 # conversion from Gwh to kwh
        ele_cum_with_co2['Monthly consumption GWh_CO2'] = (ele_cum_with_co2['guess_consumption'] * 10 ** 6 *
                                                           ele_cum_with_co2['intensity']) / 10 ** 6
        # 10**6 = conversion to tons and 10**6 # conversion from Gwh to kwh

        ele_cum_with_co2['Cumulative consumption GWh_CO2'] = ele_cum_with_co2['Monthly consumption GWh_CO2'].cumsum()

        save_cumulative_ghg_emissions(ele_cum_with_co2.to_dict('records'))

        # 1. prepare the input dataset
        energy_mix_to_merge = df3[['Entity', 'Year', 'Share of Coal', 'Share of Gas', 'Share of Oil',
                                   'Share of Nuclear', 'Share of Hydro', 'Share of Wind', 'Share of Solar',
                                   'Share of Other renewable']]
        energy_mix_to_merge = populate_co2(energy_mix_to_merge, 'Entity', 'Year', populate_until_year)
        nodeshare = eth_country_node_share.copy()
        nodeshare = nodeshare.reset_index()

        # 2. merge power source share to nodeshare dataset
        nodeshare_merge = pd.merge(nodeshare, energy_mix_to_merge, how='left',
                                   left_on=['Year', 'Country Equivalent'], right_on=['Year', 'Entity'])
        nodeshare_merge = nodeshare_merge.sort_values(['Country Equivalent', 'date'])
        nodeshare_merge = nodeshare_merge.reset_index(drop=True)

        # 4. calculate weighted share for each power source
        power_source_share = ['Share of Coal', 'Share of Gas', 'Share of Oil',
                              'Share of Nuclear', 'Share of Hydro', 'Share of Wind', 'Share of Solar',
                              'Share of Other renewable']
        for power in power_source_share:
            nodeshare_merge["Weighted " + power] = nodeshare_merge['share of global nodes'] * nodeshare_merge[power]

        weighted_source_share = []
        for power in power_source_share:
            weighted_source_share.append("Weighted " + power)

        # 5. aggregate data monthly to get monthly ethereum energy mix and make sure each month sums up to 1
        nodeshare_merge['date'] = pd.to_datetime(nodeshare_merge['date'])

        monthly_ethereum_energy_mix = nodeshare_merge.groupby('date')[weighted_source_share].sum()
        monthly_ethereum_energy_mix.sum(axis=1)

        # 6. sums up monthly data and then take an average to get yearly ethereum energy mix share
        monthly_ethereum_energy_mix = monthly_ethereum_energy_mix.reset_index(drop=False)
        monthly_ethereum_energy_mix['date'] = pd.to_datetime(monthly_ethereum_energy_mix['date'])
        monthly_ethereum_energy_mix['Year'] = monthly_ethereum_energy_mix['date'].dt.year
        monthly_ethereum_energy_mix['Month'] = monthly_ethereum_energy_mix['date'].dt.month
        yearly_ethereum_energy_mix = monthly_ethereum_energy_mix.groupby('Year')[weighted_source_share].mean()
        yearly_ethereum_energy_mix.sum(axis=1)
        monthly_ethereum_energy_mix = monthly_ethereum_energy_mix.groupby(['Year', 'Month'])[
            weighted_source_share].mean()
        monthly_ethereum_energy_mix.sum(axis=1)

        yearly_ethereum_energy_mix = yearly_ethereum_energy_mix.reset_index(drop=False)
        graph_ethereum_energy_mix = pd.melt(yearly_ethereum_energy_mix, id_vars='Year',
                                            value_vars=weighted_source_share,
                                            var_name='Power Source', value_name='Power Source Share')
        ## reformat
        graph_ethereum_energy_mix['Power Source'] = graph_ethereum_energy_mix['Power Source'].str.extract(
            'Weighted Share of (.+)')
        graph_ethereum_energy_mix['Date'] = pd.to_datetime(graph_ethereum_energy_mix['Year'].astype('str') + '-01-01')
        graph_ethereum_energy_mix = graph_ethereum_energy_mix.sort_values(['Year', 'Power Source Share'],
                                                                          ascending=[True, False])
        graph_ethereum_energy_mix['Power Source Share'] = round(graph_ethereum_energy_mix['Power Source Share'] * 100,
                                                                2)

        save_power_sources(graph_ethereum_energy_mix.to_dict('records'), 'yearly')

        monthly_ethereum_energy_mix = monthly_ethereum_energy_mix.reset_index(drop=False)
        graph_ethereum_energy_mix = pd.melt(monthly_ethereum_energy_mix, id_vars=['Year', 'Month'],
                                            value_vars=weighted_source_share,
                                            var_name='Power Source', value_name='Power Source Share')
        graph_ethereum_energy_mix['Date'] = pd.to_datetime(
            graph_ethereum_energy_mix['Year'].astype(str) + '-' + graph_ethereum_energy_mix['Month'].astype(
                str) + '-01')
        ## reformat
        graph_ethereum_energy_mix['Power Source'] = graph_ethereum_energy_mix['Power Source'].str.extract(
            'Weighted Share of (.+)')
        graph_ethereum_energy_mix = graph_ethereum_energy_mix.sort_values(['Date', 'Power Source Share'],
                                                                          ascending=[True, False])
        graph_ethereum_energy_mix['Power Source Share'] = round(graph_ethereum_energy_mix['Power Source Share'] * 100,
                                                                2)
        save_power_sources(graph_ethereum_energy_mix.to_dict('records'), 'monthly')


def populate_co2(co2_emission_df, country_col, year_col, updatest_year):
    newly_populated = []
    for country in co2_emission_df[country_col].unique():
        max_available_year = co2_emission_df[co2_emission_df[country_col] == country][year_col].max()
        max_available_record = co2_emission_df[
            (co2_emission_df[country_col] == country) & (co2_emission_df[year_col] == max_available_year)].to_dict(
            'records')[0]

        while max_available_record[year_col] < updatest_year:
            max_available_record = max_available_record.copy()  # create a copy of the dictionary
            max_available_record[year_col] += 1
            newly_populated.append(max_available_record)
    new = pd.DataFrame(newly_populated)
    total = co2_emission_df.copy()
    total = total.append(new)
    total = total.sort_values([country_col, year_col])
    total = total.reset_index(drop=True)
    return total


def save_digiconomist_live_data(cursor):
    digiconomist = Digiconomist()
    yesterday = datetime.today() - timedelta(days=1)
    result = digiconomist.ethereum(yesterday)
    if len(result) > 0:
        cursor.execute(
            'INSERT INTO digiconomist_btc ("24hr_kWh", "24hr_kgCO2", date, asset) '
            'VALUES (%s, %s, %s, %s)',
            (
                result[0]['24hr_kWh'],
                result[0]['24hr_kgCO2'],
                yesterday,
                'eth'
            )
        )
