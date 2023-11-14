from config import config

import os
import click
import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import datetime
from components.eth import EthPowFactory
from calendar import month_name

# Total Lifecycle Emissions in gCO2eq/kWh - updated version
co2_g_p_kwh_hydro = 21
co2_g_p_kwh_wind = 13
co2_g_p_kwh_nuc = 13
co2_g_p_kwh_solar = 35.5
co2_g_p_kwh_gas = 486  # Natural Gas
co2_g_p_kwh_coal = 1001
co2_g_p_kwh_oil = 840
co2_g_p_kwh_oRenew = 32.3

# The electricity mix for the following country will be treated as world general
manual_adjust_country_list = ['Gibraltar', 'Niue', 'Saint Helena', 'Western Sahara',
                              'Bermuda', 'Andorra', 'Curacao',
                              'Macau SAR', 'Liechtenstein', 'Monaco', 'San Marino']

# CCAF mining map start/end time
map_start_date = '2019-09-01'
map_end_date = '2022-01-01'
us_map_start_date = '2021-12-01'
this_year = datetime.now().year

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

mining_map_continents = {
    'China': 'Asia',
    'Korea, Rep.': 'Asia',
    'Singapore': 'Asia',
    'Russian Federation': 'Europe',
    'Germany': 'Europe',
    'Sweden': 'Europe',
    'Netherlands': 'Europe',
    'Belgium': 'Europe',
    'Austria': 'Europe',
    'United States': 'North America',
    'Canada': 'North America',
}


@click.command(name='eth-pow:ghg-emissions')
def handle():
    with psycopg2.connect(**config['custom_data']) as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("select id, country from countries")
        countries = cursor.fetchall()

        def save_intensity(records, name):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into co2_coefficients '
                    '(asset, name, timestamp, date, co2_coef) '
                    'values %s on conflict (timestamp, asset) do nothing',
                    list([(
                        'eth_pow',
                        name,
                        int(datetime.strptime(x['Date and Time'], "%Y-%m-%d").timestamp()),
                        x['Date and Time'],
                        x['intensity'],
                    ) for x in records])
                )

        def save_ghg_emissions(records, price, name, value_key):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into greenhouse_gas_emissions '
                    '(asset, price, name, timestamp, date, value) '
                    'values %s on conflict (asset, price, date, name) do nothing',
                    list([(
                        'eth_pow',
                        price,
                        name,
                        int(x['Date and Time'].timestamp()),
                        x['Date and Time'].strftime("%Y-%m-%d"),
                        x[value_key],
                    ) for x in records])
                )

        def save_cumulative_ghg_emissions(records, price_value):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into cumulative_greenhouse_gas_emissions '
                    '(asset, price, timestamp, date, value, cumulative_value) '
                    'values %s on conflict (asset, price, date) do nothing',
                    list([(
                        'eth_pow',
                        price_value,
                        int(datetime.strptime(x['Month'], "%Y-%m-%d").timestamp()),
                        x['Month'],
                        x['Monthly consumption TWh_CO2'],
                        x['Cumulative consumption TWh_CO2'],
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
                        'eth_pow',
                        power_type,
                        x['Power Source'],
                        int(x['Date'].timestamp()),
                        x['Date'].strftime("%Y-%m-%d"),
                        x['Power Source Share'],
                        power_sources_order[x['Power Source']]
                    ) for x in records])
                )

        def save_geo_distribution(records):
            if len(records):
                psycopg2.extras.execute_values(
                    cursor,
                    'insert into eth_pow_distribution '
                    '(country_id, name, value, date, continent)  '
                    'values %s on conflict (name, date) do nothing',
                    list([(
                        next((c['id'] for c in countries if c['country'] == x['country']), None),
                        x['country'],
                        x['value'],
                        x['date'].strftime("%Y-%m-%d"),
                        mining_map_continents[x['country']] if x['country'] in mining_map_continents else 'Others'
                    ) for x in records])
                )

        region_mix = load_world_region_electricity_mix()
        us_generation_mix = load_us_region_electricity_mix()
        eth_miner_share_merged = load_eth_miner_country_distribution()
        mining_map_countries = load_btc_mining_map_countries(cursor)
        mining_map_provinces = load_btc_mining_map_states(cursor)
        df3 = load_electricity_mix_pct_countries()
        china_powermix = load_electricity_mix_pct_china_provinces()
        us_electricity_mix_wide = load_electricity_mix_pct_us_states()

        eth_miner_share_total = merge_btc_mining_map(eth_miner_share_merged, mining_map_countries, mining_map_provinces)
        eth_miner_share_total_filled = merge_electricity_mix_pct(
            eth_miner_share_total,
            df3,
            us_electricity_mix_wide,
            china_powermix,
        )
        eth_miner_share_total_filled = adjust_asia_region_china_electricity_mix(eth_miner_share_total_filled)
        eth_miner_share_total_filled = adjust_north_america_region_us_electricity_mix(eth_miner_share_total_filled)
        eth_miner_share_total_filled = calculate_weighted_country_share_within_region(
            eth_miner_share_total_filled,
            us_generation_mix,
            region_mix,
        )

        co2_intensity = eth_miner_share_total_filled.groupby('date', as_index=False)['intensity'].sum()
        co2_intensity.rename({'date': 'monthly date'}, axis=1, inplace=True)

        df4 = co2_intensity.copy()
        df4.rename({'monthly date': 'Date and Time'}, axis=1, inplace=True)
        df4 = df4.reset_index()

        his_graph_data = df4[df4['Date and Time'] <= map_start_date]
        future_graph_data = df4[df4['Date and Time'] >= map_end_date]
        df4 = df4[(df4['Date and Time'] >= map_start_date) & (df4['Date and Time'] <= map_end_date)]

        save_intensity(his_graph_data.to_dict('records'), 'Historical')
        save_intensity(df4.to_dict('records'), 'Assessed')
        save_intensity(future_graph_data.to_dict('records'), 'Predicted')

        for cents in range(1, 21):
            co2_intensity_cp = co2_intensity.copy()
            price = round(cents / 100, 2)
            df2 = load_eth_electricity(price)
            ele_cum = load_eth_electricity_cumulative(price)

            graph_data = calculate_co2_emission(co2_intensity_cp, df2)
            df4 = graph_data.copy()
            df4 = df4.reset_index()
            his_graph_data = df4[df4['Date and Time'] <= map_start_date]
            post_elec = df4[df4['Date and Time'] >= map_end_date]
            df4 = df4[(df4['Date and Time'] >= map_start_date) & (df4['Date and Time'] <= map_end_date)]

            save_ghg_emissions(his_graph_data.to_dict('records'), str(price), 'Historical Hydro-only', 'MIN_CO2')
            save_ghg_emissions(his_graph_data.to_dict('records'), str(price), 'Historical Coal-only', 'MAX_CO2')
            save_ghg_emissions(his_graph_data.to_dict('records'), str(price), 'Historical Estimated', 'GUESS_CO2')

            save_ghg_emissions(df4.to_dict('records'), str(price), 'Assessed Hydro-only', 'MIN_CO2')
            save_ghg_emissions(df4.to_dict('records'), str(price), 'Assessed Coal-only', 'MAX_CO2')
            save_ghg_emissions(df4.to_dict('records'), str(price), 'Assessed Estimated', 'GUESS_CO2')

            save_ghg_emissions(post_elec.to_dict('records'), str(price), 'Predicted Hydro-only', 'MIN_CO2')
            save_ghg_emissions(post_elec.to_dict('records'), str(price), 'Predicted Coal-only', 'MAX_CO2')
            save_ghg_emissions(post_elec.to_dict('records'), str(price), 'Predicted Estimated', 'GUESS_CO2')

            ele_cum['Month'] = ele_cum['Month'].astype('str')
            co2_intensity_cp.rename({'monthly date': 'Date and Time'}, axis=1, inplace=True)
            ele_cum_with_co2 = pd.merge(ele_cum, co2_intensity_cp, how='left', left_on='Month', right_on='Date and Time')
            ele_cum_with_co2['Monthly consumption TWh_CO2'] = (ele_cum_with_co2['Monthly consumption, TWh'] * 10 ** 9 *
                                                               ele_cum_with_co2['intensity']) / 1000000000000
            # 10**6 = conversion to tons and 10**9 # conversion from twh to kwh
            ele_cum_with_co2['Cumulative consumption TWh_CO2'] = ele_cum_with_co2[
                'Monthly consumption TWh_CO2'].cumsum()

            save_cumulative_ghg_emissions(ele_cum_with_co2.to_dict('records'), str(price))

        eth_miner_share_total_filled['Weighted Share of Coal'] = eth_miner_share_total_filled['Share of Coal'] * \
                                                                 eth_miner_share_total_filled['btc miner share std'] * \
                                                                 eth_miner_share_total_filled[
                                                                     'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Nuclear'] = eth_miner_share_total_filled['Share of Nuclear'] * \
                                                                    eth_miner_share_total_filled[
                                                                        'btc miner share std'] * \
                                                                    eth_miner_share_total_filled[
                                                                        'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Hydro'] = eth_miner_share_total_filled['Share of Hydro'] * \
                                                                  eth_miner_share_total_filled['btc miner share std'] * \
                                                                  eth_miner_share_total_filled[
                                                                      'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Wind'] = eth_miner_share_total_filled['Share of Wind'] * \
                                                                 eth_miner_share_total_filled['btc miner share std'] * \
                                                                 eth_miner_share_total_filled[
                                                                     'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Solar'] = eth_miner_share_total_filled['Share of Solar'] * \
                                                                  eth_miner_share_total_filled['btc miner share std'] * \
                                                                  eth_miner_share_total_filled[
                                                                      'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Gas'] = eth_miner_share_total_filled['Share of Gas'] * \
                                                                eth_miner_share_total_filled['btc miner share std'] * \
                                                                eth_miner_share_total_filled['miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Other renewable'] = eth_miner_share_total_filled[
                                                                                'Share of Other renewable'] * \
                                                                            eth_miner_share_total_filled[
                                                                                'btc miner share std'] * \
                                                                            eth_miner_share_total_filled[
                                                                                'miner distribution share']
        eth_miner_share_total_filled['Weighted Share of Oil'] = eth_miner_share_total_filled['Share of Oil'] * \
                                                                eth_miner_share_total_filled['btc miner share std'] * \
                                                                eth_miner_share_total_filled['miner distribution share']

        monthly_energy_mix = eth_miner_share_total_filled.groupby('date', as_index=False)[
            ['Weighted Share of Coal', 'Weighted Share of Nuclear',
             'Weighted Share of Hydro',
             'Weighted Share of Wind',
             'Weighted Share of Solar',
             'Weighted Share of Gas',
             'Weighted Share of Other renewable',
             'Weighted Share of Oil']].sum()
        monthly_energy_mix.rename({
            'Weighted Share of Coal': 'Share of Coal',
            'Weighted Share of Nuclear': 'Share of Nuclear',
            'Weighted Share of Hydro': 'Share of Hydro',
            'Weighted Share of Wind': 'Share of Wind',
            'Weighted Share of Solar': 'Share of Solar',
            'Weighted Share of Gas': 'Share of Gas',
            'Weighted Share of Other renewable': 'Share of Other renewable',
            'Weighted Share of Oil': 'Share of Oil'
        }, axis=1, inplace=True)

        monthly_energy_mix['date'] = pd.to_datetime(monthly_energy_mix['date'])
        power_source_share = ['Share of Coal', 'Share of Gas', 'Share of Oil',
                              'Share of Nuclear', 'Share of Hydro', 'Share of Wind', 'Share of Solar',
                              'Share of Other renewable']
        monthly_energy_mix['Year'] = monthly_energy_mix['date'].dt.year
        monthly_energy_mix['Month'] = monthly_energy_mix['date'].dt.month
        yearly_energy_mix = monthly_energy_mix.groupby('Year')[power_source_share].mean()
        yearly_ethereum_energy_mix = yearly_energy_mix.reset_index(drop=False)
        graph_ethereum_energy_mix = pd.melt(yearly_ethereum_energy_mix, id_vars='Year', value_vars=power_source_share,
                                            var_name='Power Source', value_name='Power Source Share')
        ## reformat
        graph_ethereum_energy_mix['Power Source'] = graph_ethereum_energy_mix['Power Source'].str.extract(
            'Share of (.+)')
        graph_ethereum_energy_mix['Year'] = pd.to_datetime(graph_ethereum_energy_mix['Year'].astype('str') + '-01-01')
        graph_ethereum_energy_mix = graph_ethereum_energy_mix.sort_values(['Year', 'Power Source Share'],
                                                                          ascending=[True, False])
        graph_ethereum_energy_mix['Power Source Share'] = round(graph_ethereum_energy_mix['Power Source Share'] * 100,
                                                                2)
        graph_ethereum_energy_mix['Date'] = pd.to_datetime(graph_ethereum_energy_mix['Year'])

        save_power_sources(graph_ethereum_energy_mix.to_dict('records'), 'yearly')

        monthly_ethereum_energy_mix = monthly_energy_mix.groupby(['Year', 'Month'])[power_source_share].mean()
        monthly_ethereum_energy_mix = monthly_ethereum_energy_mix.reset_index(drop=False)
        graph_ethereum_energy_mix = pd.melt(monthly_ethereum_energy_mix, id_vars=['Year', 'Month'],
                                            value_vars=power_source_share,
                                            var_name='Power Source', value_name='Power Source Share')
        graph_ethereum_energy_mix['Power Source'] = graph_ethereum_energy_mix['Power Source'].str.extract(
            'Share of (.+)')
        graph_ethereum_energy_mix['Date'] = pd.to_datetime(
            graph_ethereum_energy_mix['Year'].astype(str) + '-' + graph_ethereum_energy_mix['Month'].astype(
                str) + '-01')
        graph_ethereum_energy_mix = graph_ethereum_energy_mix.sort_values(['Date', 'Power Source Share'],
                                                                          ascending=[True, False])
        graph_ethereum_energy_mix['Power Source Share'] = round(graph_ethereum_energy_mix['Power Source Share'] * 100,
                                                                2)
        save_power_sources(graph_ethereum_energy_mix.to_dict('records'), 'monthly')

        select_country_list = ['CHN', 'USA', 'KOR', 'SGP', 'RUS', 'DEU', 'SWE', 'NLD', 'BEL', 'AUT', 'CAN']
        is_monthly = True
        eth_country_distribution = eth_miner_share_total_filled.copy()
        if is_monthly:
            eth_country_distribution['date'] = eth_country_distribution['monthly-time']
        # 1. change country/state to China and US
        filt_1 = (eth_country_distribution['level'] == 'states') & (eth_country_distribution['Code'] == 'CHN')
        filt_2 = (eth_country_distribution['level'] == 'states') & (eth_country_distribution['Code'] == 'USA')
        eth_country_distribution.loc[filt_1, 'country/state'] = 'China'
        eth_country_distribution.loc[filt_2, 'country/state'] = 'United States'

        # 2. calculate simple average for each country within each region

        eth_country_distribution['miner_country_share'] = eth_country_distribution['btc miner share std'] * \
                                                          eth_country_distribution['miner distribution share']
        output = eth_country_distribution.groupby(['date', 'country/state', 'Code'], as_index=False)[
            'miner_country_share'].sum()

        filt_3 = [x in select_country_list for x in output['Code']]
        output_select = output.loc[filt_3]
        output_other = output.drop(output_select.index)
        output_other = output_other.groupby('date', as_index=False)['miner_country_share'].sum()
        output_other['Code'] = 'OTHER'
        output_other['country/state'] = 'Others'
        output_graph = output_select.append(output_other)
        output_graph = output_graph.sort_values(['date', 'miner_country_share'], ascending=[True, False])
        output_graph = pd.pivot(output_graph, index='date', columns='country/state', values='miner_country_share')
        col_to_move = 'Others'
        output_graph = output_graph[[col for col in output_graph.columns if col != col_to_move] + [col_to_move]]

        output_graph.reset_index(inplace=True)
        melted_df = output_graph.melt(id_vars='date', var_name='country', value_name='value')
        melted_df.dropna(subset=['value'], inplace=True)

        save_geo_distribution(melted_df.to_dict('records'))


def split(item):
    return item.split('T')[0]


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


def populate_df(df, year):
    data = df.copy()
    data_max_year = data[year].max()
    current_year = datetime.now().year
    storage = []
    filt = data[year] == data_max_year
    for i in range(data_max_year + 1, current_year + 1):
        tempt = data.loc[filt]
        tempt[year] = i
        storage.append(tempt)
    new_data = pd.concat(storage)
    data = data.append(new_data)
    return data


def populate_intensity_df(df, country, year):
    # end year differently
    data = df.copy()
    data_max_year = data[year].max()
    data_max_minus_one = data[year].max() - 1
    current_year = datetime.now().year
    storage = []
    filt = data[year] == data_max_year
    for i in range(data_max_year + 1, current_year + 1):
        tempt = data.loc[filt]
        tempt[year] = i
        storage.append(tempt)
    new_data = pd.concat(storage)
    data = data.append(new_data)

    filt_2 = data[year] == data_max_minus_one
    old_country_list = data.loc[filt_2, country].unique()
    new_country_list = data.loc[filt, country].unique()

    to_add_country_list = [x for x in old_country_list if x not in new_country_list]

    old_country_df = data.loc[filt_2]
    filt_3 = [x in to_add_country_list for x in old_country_df[country]]
    storage = []
    for i in range(data_max_year, current_year + 1):
        tempt = old_country_df.loc[filt_3]
        tempt[year] = i
        storage.append(tempt)
    new_data = pd.concat(storage)
    data = data.append(new_data)

    return data


def populate_co2(co2_emission_df, country_col, year_col, updatest_year):
    # this function is used to populate the dataframe using the last available data of a given country
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


def load_world_region_electricity_mix():
    '''
    this function is usded to calculate total electricity generation in different countries
    and populate the data up to this year using the last available data points
    '''

    region_mix = pd.read_csv(os.getcwd() + '/../storage/eth_pow/electricity-prod-source-stacked.csv')
    cols = ['Other renewables excluding bioenergy (TWh) (zero filled)',
            'Electricity from bioenergy (TWh) (zero filled)',
            'Electricity from solar (TWh)', 'Electricity from wind (TWh)',
            'Electricity from hydro (TWh)', 'Electricity from nuclear (TWh)',
            'Electricity from oil (TWh)', 'Electricity from gas (TWh)',
            'Electricity from coal (TWh)']
    region_mix['Total Electricity (TWh)'] = 0
    for col in cols:
        region_mix['Total Electricity (TWh)'] = region_mix['Total Electricity (TWh)'] + region_mix[col]
    region_mix = region_mix[['Entity', 'Code', 'Year', 'Total Electricity (TWh)']]
    region_mix = populate_co2(region_mix, 'Entity', 'Year', this_year)
    return region_mix


def load_us_region_electricity_mix():
    '''
    this function is used to calculate United States state-level electricity generation and
    populate the data up to this year using the last available data points
    '''
    us_generation_mix = pd.read_csv(
        os.getcwd() + '/../storage/eth_pow/updated-electricity-prod-source-stacked-US-state.csv')
    us_generation_mix = us_generation_mix[us_generation_mix['ENERGY SOURCE'] == 'Total']
    us_generation_mix = us_generation_mix.drop(['TYPE OF PRODUCER', 'ENERGY SOURCE', 'ENERGY SHARE'], axis=1)
    us_generation_mix = us_generation_mix.rename({'YEAR': 'Year', 'STATE': 'province_code'}, axis=1)
    us_generation_mix = populate_co2(us_generation_mix, 'province_code', 'Year', this_year)
    return us_generation_mix


def load_eth_miner_country_distribution():
    '''
    this function is used to create a dataframe to store daily eth miner country-level distribution
    (based on Region-Country classification system)
    '''
    # Miner geographic share of each country (eth pow)
    country_classification = pd.read_csv(
        os.getcwd() + '/../storage/eth_pow/updated_country_classification_clean.csv')
    eth_miner_share = pd.read_csv(
        os.getcwd() + '/../storage/eth_pow/updated_daily_region_percent.csv')

    # Add country classificatin to eth miner share
    eth_miner_share_long = pd.melt(eth_miner_share, id_vars='time', var_name='region',
                                   value_name='miner distribution share')
    eth_miner_share_long = eth_miner_share_long[eth_miner_share_long['region'] != 'total']
    eth_miner_share_long = eth_miner_share_long[eth_miner_share_long['miner distribution share'] > 0]
    eth_miner_share_merged = pd.merge(eth_miner_share_long, country_classification, how='left', on='region')
    eth_miner_share_merged['time'] = pd.to_datetime(eth_miner_share_merged['time'].astype('str') + '-01')

    filt = pd.notnull(eth_miner_share_merged['country']) & (eth_miner_share_merged['level'] != 'states')
    eth_miner_share_merged.loc[filt, 'country/state'] = eth_miner_share_merged.loc[filt, 'country']
    eth_miner_share_merged.drop('country', axis=1, inplace=True)
    eth_miner_share_merged['monthly-time'] = pd.to_datetime(eth_miner_share_merged['time'].dt.year.astype('str') + '-' +
                                                            eth_miner_share_merged['time'].dt.month.astype(
                                                                'str') + '-01')
    return eth_miner_share_merged


def load_btc_mining_map_countries(cursor):
    '''
    this function is used to load CCAF Bitcoin mining map (countries hashrate distribution)
    '''
    ## Get Bitcoin miner distribution - country level
    cursor.execute('select * from mining_map_countries')
    mining_map_countries = pd.DataFrame(cursor.fetchall())
    mining_map_countries.rename({'value': 'btc miner share', 'date': 'time'}, axis=1, inplace=True)
    mining_map_countries['time'] = pd.to_datetime(mining_map_countries['time'])
    mining_map_countries.rename({'time': 'monthly-time'}, axis=1, inplace=True)
    mining_map_countries.drop('id', axis=1, inplace=True)
    return mining_map_countries


def load_btc_mining_map_states(cursor):
    '''
    this function is used to load CCAF Bitcoin mining map (state-level for United States and China)
    '''
    ## Get Bitcoin miner distribution - state level
    cursor.execute('select * from mining_map_provinces')
    mining_map_provinces = pd.DataFrame(cursor.fetchall())
    mining_map_provinces = mining_map_provinces.drop(['id', 'value'], axis=1)
    mining_map_provinces.rename({'local_value': 'btc miner share',
                                 'code': 'province_code',
                                 'date': 'time'}, axis=1, inplace=True)
    mining_map_provinces['time'] = pd.to_datetime(mining_map_provinces['time'])
    mining_map_us_provinces = mining_map_provinces[mining_map_provinces['country_id'] != 129]
    mining_map_cn_provinces = mining_map_provinces[mining_map_provinces['country_id'] == 129]
    selected_provinces = ['Sichuan', 'Xinjiang', 'Yunnan', 'Inner Mongolia', 'Gansu', 'Beijing', 'Shanxi', 'Qinghai']
    filt = [x in selected_provinces for x in mining_map_cn_provinces['name']]
    mining_map_cn_selected_provinces = mining_map_cn_provinces.loc[filt]
    filt = [x not in selected_provinces for x in mining_map_cn_provinces['name']]
    mining_map_cn_other_provinces = mining_map_cn_provinces.loc[filt]
    mining_map_cn_other_provinces = mining_map_cn_other_provinces.groupby(['time', 'country_id'],
                                                                          as_index=False)['btc miner share'].sum()
    mining_map_cn_other_provinces['name'] = 'Other'
    mining_map_cn_other_provinces['province_code'] = 'CHINA-OTHER'
    mining_map_provinces = pd.concat([mining_map_us_provinces, mining_map_cn_selected_provinces,
                                      mining_map_cn_other_provinces])
    mining_map_provinces.rename({'time': 'monthly-time'}, axis=1, inplace=True)
    return mining_map_provinces


def load_eth_electricity(price):
    '''
    this functin is used to load CCAF daily Ethereum electricity and power estimtes
    '''
    # Electricity and power Estimates
    df2 = pd.DataFrame([
        {
            'Date and Time': datetime.utcfromtimestamp(x['timestamp']).isoformat(),
            'power MIN, GW': round(x['min_power'], 4),
            'power GUESS, GW': round(x['guess_power'], 4),
            'power MAX, GW': round(x['max_power'], 4),
            'annualised consumption MIN, TWh': round(x['min_consumption'], 4),
            'annualised consumption GUESS, TWh': round(x['guess_consumption'], 4),
            'annualised consumption MAX, TWh': round(x['max_consumption'], 4),
        }
        for x in EthPowFactory.create_repository().get_network_power_demand(price)
    ])

    df2['date'] = pd.to_datetime(df2['Date and Time'])

    df2.drop('date', axis=1, inplace=True)
    df2 = df2.reset_index(drop=True)
    return df2


def load_electricity_mix_pct_countries():
    '''
    this function is used to load country-level electricity mix (percentage)
    '''
    df3 = pd.read_csv(
        os.getcwd() + '/../storage/eth_pow/share-elec-by-source.csv')

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
    df3 = populate_intensity_df(df3, 'Code', 'Year')
    return df3


def load_electricity_mix_pct_china_provinces():
    '''
    this function is used to load China provicial-level electricity mix (percentage)
    '''
    # Electricity mix for provinces in China source ###
    prov = pd.read_excel(
        os.getcwd() + '/../storage/eth_pow/electricity-prod-source-stacked-CN-province.xlsx')

    # create pivot table
    prov_wide = pd.pivot(prov, index=['Province', 'Year'],
                         columns=['Electricity Consumption'],
                         values=['Amount (Unit: Billion kWh)'])['Amount (Unit: Billion kWh)'].reset_index(drop=False)
    prov_wide.columns.name = None
    prov_wide['Year'] = prov_wide['Year'].astype('str')
    prov_wide['Year'] = prov_wide['Year'].str.extract('(\d{4}$)')
    prov_wide['Year'] = prov_wide['Year'].astype('int')

    # create month data when mining map is available
    timetable = pd.DataFrame(pd.date_range(start=map_start_date, end=map_end_date, freq='1M'), columns=['End of Month'])
    timetable['Date and Time'] = timetable['End of Month'].dt.year.astype('str') + '-' + timetable[
        'End of Month'].dt.month.astype('str') + '-01'
    timetable['Date and Time'] = pd.to_datetime(timetable['Date and Time'])
    timetable['Year'] = timetable['Date and Time'].dt.year
    timetable = timetable[['Date and Time', 'Year']]
    prov_season_table = pd.merge(timetable, prov_wide, how='left', on=['Year'])

    china_powermix = prov_season_table.copy()
    china_powermix['Total'] = china_powermix['Hydro'] + china_powermix['Nuclear'] + china_powermix['Solar'] + \
                              china_powermix['Thermal'] + china_powermix['Wind']
    china_powermix['Share of Coal'] = china_powermix['Thermal'] / china_powermix['Total']
    china_powermix['Share of Nuclear'] = china_powermix['Nuclear'] / china_powermix['Total']
    china_powermix['Share of Hydro'] = china_powermix['Hydro'] / china_powermix['Total']
    china_powermix['Share of Wind'] = china_powermix['Wind'] / china_powermix['Total']
    china_powermix['Share of Solar'] = china_powermix['Solar'] / china_powermix['Total']
    china_powermix.drop(['Hydro', 'Nuclear', 'Solar', 'Thermal', 'Wind', 'Total'], axis=1, inplace=True)
    china_powermix.rename({'Date and Time': 'date',
                           'Province': 'country/state'}, axis=1, inplace=True)
    return china_powermix


def load_electricity_mix_pct_us_states():
    '''
    this function is used to load United Stats state-level electricity-mix (percentage)
    '''
    us_electricity_mix = pd.read_csv(
        os.getcwd() + '/../storage/eth_pow/updated-electricity-prod-source-stacked-US-state.csv')

    # create pivot table
    us_electricity_mix_wide = pd.pivot(us_electricity_mix, index=['STATE', 'YEAR'],
                                       columns=['ENERGY SOURCE'],
                                       values=['ENERGY SHARE'])['ENERGY SHARE'].reset_index(drop=False)
    us_electricity_mix_wide.columns.name = None

    us_elec_rename = {'YEAR': 'Year',
                      'Hydro': 'Share of Hydro',
                      'Nuclear': 'Share of Nuclear',
                      'Solar': 'Share of Solar',
                      'Coal': 'Share of Coal',
                      'Wind': 'Share of Wind',
                      'Gas': 'Share of Gas',
                      'Hydroelectric': 'Share of Hydro',
                      'Oil': 'Share of Oil',
                      'Solar Thermal and Photovoltaic': 'Share of Solar',
                      'Other': 'Share of Other renewable',
                      'STATE': 'province_code'}
    us_electricity_mix_wide.rename(us_elec_rename, axis=1, inplace=True)

    # populate us_electricity_mix_wide uptill current year (forward fill)
    us_electricity_mix_wide = populate_df(us_electricity_mix_wide, 'Year')
    us_electricity_mix_wide.drop('Total', axis=1, inplace=True)
    return us_electricity_mix_wide


def load_eth_electricity_cumulative(price):
    '''
    this function is used to load CCAF cumulative eth electricity estimate
    '''
    # Cumulative Electricity consumed
    ele_cum = pd.DataFrame([
        {
            'Month': month_name[datetime.fromtimestamp(x['timestamp']).month] + datetime.fromtimestamp(
                x['timestamp']).strftime(' %Y'),
            'Monthly consumption, TWh': round(x['guess_consumption'], 4),
            'Cumulative consumption, TWh': round(x['cumulative_guess_consumption'], 4),
        }
        for x in EthPowFactory.create_repository().get_monthly_total_electricity_consumption(price)
    ])

    ele_cum['Month'] = pd.to_datetime(ele_cum['Month'])
    return ele_cum


def merge_btc_mining_map(eth_miner_share_merged, mining_map_countries, mining_map_provinces):
    '''
    this function is used to merge bitcoin mining map country distribution to eth miner region distribution dataframe
    including merge for state-level data in US and China
    for mining map period (2019.09-2021.01)
    '''
    eth_miner_share_merged = pd.merge(eth_miner_share_merged, mining_map_countries, how='left',
                                      on=['monthly-time', 'country_id'])
    filt = eth_miner_share_merged['level'] == 'countries'
    eth_miner_share_countries = eth_miner_share_merged.loc[filt]

    filt = eth_miner_share_merged['level'] == 'states'
    eth_miner_share_states = eth_miner_share_merged.loc[filt]
    eth_miner_share_states.drop(['name', 'btc miner share'], axis=1, inplace=True)
    eth_miner_share_states = pd.merge(eth_miner_share_states, mining_map_provinces,
                                      how='left', on=['monthly-time', 'country_id', 'province_code'])
    ## Append country and state data together
    eth_miner_share_total = eth_miner_share_countries.append(eth_miner_share_states)
    eth_miner_share_total = eth_miner_share_total.sort_values(['time', 'region'])

    ## Standardize bit miner share
    eth_miner_share_total['btc miner share'].fillna(0)
    eth_miner_share_total['btc miner monthly total'] = eth_miner_share_total.groupby(['time', 'region'])[
        'btc miner share'].transform("sum")
    eth_miner_share_total['btc miner share std'] = eth_miner_share_total['btc miner share'] / eth_miner_share_total[
        'btc miner monthly total']
    eth_miner_share_total.drop(['btc miner share', 'btc miner monthly total', 'name'], axis=1, inplace=True)
    eth_miner_share_total.rename({'time': 'date', 'code3': 'Code'}, axis=1, inplace=True)

    ## Manually adjusted countries
    filt = [x in manual_adjust_country_list for x in eth_miner_share_total['country/state']]
    eth_miner_share_total.loc[filt, 'Code'] = 'OWID_WRL'
    eth_miner_share_total['Year'] = eth_miner_share_total['date'].dt.year

    return eth_miner_share_total


def merge_electricity_mix_pct(eth_miner_share_total, df3, us_electricity_mix_wide, china_powermix):
    '''
    this function is used to merge electricity mix percentage
    (including world country-level, US state-level, China provincial-level)
    to eth miner region distribution dataframe for all time period
    '''
    eth_miner_share_states = eth_miner_share_total[eth_miner_share_total['level'] == 'states']
    eth_miner_share_countries = eth_miner_share_total[eth_miner_share_total['level'] == 'countries']

    # Merge with United States state-level energy mix
    eth_miner_share_us_states = eth_miner_share_states[eth_miner_share_states['region'] != 'china']
    eth_miner_share_us_states = pd.merge(eth_miner_share_us_states, us_electricity_mix_wide, how='left',
                                         on=['Year', 'province_code'])
    unmatched_filt = pd.isnull(eth_miner_share_us_states['Share of Coal'])
    unmatched_states = eth_miner_share_us_states.loc[unmatched_filt].iloc[:, :-8]
    unmatched_states = pd.merge(unmatched_states, df3, how='left', on=['Year', 'Code'])
    unmatched_states.drop('Entity', axis=1, inplace=True)
    eth_miner_share_us_states = eth_miner_share_us_states.append(unmatched_states)
    eth_miner_share_us_states = eth_miner_share_us_states[pd.notnull(eth_miner_share_us_states['Share of Coal'])]
    china_powermix.rename({'date': 'monthly-time'}, axis=1, inplace=True)

    # Merge with China province-level energy mix
    eth_miner_share_cn_states = eth_miner_share_states[eth_miner_share_states['region'] == 'china']
    eth_miner_share_cn_states = pd.merge(eth_miner_share_cn_states, china_powermix, how='left',
                                         on=['monthly-time', 'Year', 'country/state'])
    filt = pd.isnull(eth_miner_share_cn_states['Share of Coal'])
    unmatched_provinces = eth_miner_share_cn_states.loc[filt].iloc[:, :-5]
    unmatched_provinces = pd.merge(unmatched_provinces, df3, on=['Code', 'Year'])
    unmatched_provinces.drop('Entity', axis=1, inplace=True)
    eth_miner_share_cn_states = eth_miner_share_cn_states.append(unmatched_provinces)
    filt = pd.notnull(eth_miner_share_cn_states['Share of Coal'])
    eth_miner_share_cn_states = eth_miner_share_cn_states.loc[filt].sort_values(['date', 'country/state'])
    eth_miner_share_cn_states['Share of Gas'] = eth_miner_share_cn_states['Share of Gas'].fillna(0)
    eth_miner_share_cn_states['Share of Oil'] = eth_miner_share_cn_states['Share of Oil'].fillna(0)
    eth_miner_share_cn_states['Share of Other renewable'] = eth_miner_share_cn_states[
        'Share of Other renewable'].fillna(0)
    eth_miner_share_states_filled = eth_miner_share_us_states.append(eth_miner_share_cn_states)

    # Merge with country-level energy mix
    eth_miner_share_countries = pd.merge(eth_miner_share_countries, df3, how='left', on=['Code', 'Year'])
    eth_miner_share_countries.drop('Entity', axis=1, inplace=True)
    eth_miner_share_total_filled = pd.concat([eth_miner_share_countries, eth_miner_share_states_filled])
    eth_miner_share_total_filled = eth_miner_share_total_filled.sort_values(['date', 'region'])
    eth_miner_share_total_filled['date'] = pd.to_datetime(eth_miner_share_total_filled['date']).dt.date.astype("str")
    return eth_miner_share_total_filled


def adjust_asia_region_china_electricity_mix(eth_miner_share_total_filled):
    '''
    this function is used to adjust electricity mix China within Asia, Asia-east region
    in mining map period (2019.09-2020.06)
    (use provincial-level Chinese btc miner distribution as weight for each provice and aggregate to get the
    country-level electricity mix for each month)
    '''
    filt = ((eth_miner_share_total_filled['monthly-time'] >= map_start_date)
            & (eth_miner_share_total_filled['monthly-time'] <= map_end_date)
            & (eth_miner_share_total_filled['region'] == 'china'))

    ## calculate China aggregate co2 coefficient in mining map period
    ## 2019.09 - 2022.01
    china_co2_map = eth_miner_share_total_filled.loc[filt]

    ## 1. get the china provincial data start date & end date
    relevant_time = pd.notnull(china_co2_map['btc miner share std'])
    china_provincial_start_date = china_co2_map.loc[relevant_time, 'date'].min()
    china_provincial_end_date = china_co2_map.loc[relevant_time, 'date'].max()

    ## 1.1 apply a filter to select china(region) data only (during the above period when china data is available)
    filt = ((china_co2_map['date'] >= china_provincial_start_date) &
            (china_co2_map['date'] <= china_provincial_end_date))
    sub_china_co2_map = china_co2_map.loc[filt]

    ## 1.2 calculate weighted share of for each day:
    ## Share of Coal (country-level) = Sum of all provinces (Share of Coal * btc miner share std)
    cols = ['Share of Coal', 'Share of Gas',
            'Share of Hydro', 'Share of Solar', 'Share of Wind', 'Share of Oil',
            'Share of Nuclear', 'Share of Other renewable']
    for col in cols:
        sub_china_co2_map['weighted' + ' ' + col] = sub_china_co2_map['btc miner share std'] * sub_china_co2_map[col]

    to_merge_china_co2_map = sub_china_co2_map.groupby(['date', 'region'], as_index=False)['weighted Share of Coal',
    'weighted Share of Gas',
    'weighted Share of Hydro', 'weighted Share of Solar',
    'weighted Share of Wind', 'weighted Share of Oil',
    'weighted Share of Nuclear', 'weighted Share of Other renewable'].sum()

    to_merge_china_co2_map.columns = ['date', 'country/state', 'Share of Coal', 'Share of Gas',
                                      'Share of Hydro', 'Share of Solar', 'Share of Wind', 'Share of Oil',
                                      'Share of Nuclear', 'Share of Other renewable']
    to_merge_china_co2_map['country/state'] = 'China'

    ## 1.3 apply a filter of same data period to select rows to change
    filt = ((eth_miner_share_total_filled['date'] >= china_provincial_start_date) &
            (eth_miner_share_total_filled['date'] <= china_provincial_end_date) &
            (eth_miner_share_total_filled['country/state'] == 'China'))
    to_change_china_df = eth_miner_share_total_filled.loc[filt]
    eth_miner_share_total_filled = eth_miner_share_total_filled.drop(
        to_change_china_df.index)  ##exclude the selected rows
    to_change_china_df.drop(cols, axis=1, inplace=True)

    ## 1.4 merge two dataframes together
    to_change_china_df = pd.merge(to_change_china_df, to_merge_china_co2_map, how='left',
                                  on=['date', 'country/state'])

    ## 1.5 append back total dataframe and sort
    eth_miner_share_total_filled = eth_miner_share_total_filled.append(to_change_china_df)
    eth_miner_share_total_filled = eth_miner_share_total_filled.sort_values(['date', 'region'])
    return eth_miner_share_total_filled


def adjust_north_america_region_us_electricity_mix(eth_miner_share_total_filled):
    '''
    this function is used to adjust electricity mix US within North-America region
    in mining map period (2021.12)
    (use provincial-level US btc miner distribution as weight for each provice and aggregate to get the
    country-level electricity mix for each month)
    '''
    filt = ((eth_miner_share_total_filled['monthly-time'] >= map_start_date)
            & (eth_miner_share_total_filled['monthly-time'] <= map_end_date)
            & (eth_miner_share_total_filled['region'] == 'us'))

    ## calculate US aggregate co2 coefficient in mining map period
    ## 2019.09 - 2022.01
    us_co2_map = eth_miner_share_total_filled.loc[filt]

    ## 1. get the us provincial data start date & end date
    relevant_time = pd.notnull(us_co2_map['btc miner share std'])
    us_provincial_start_date = us_co2_map.loc[relevant_time, 'date'].min()
    us_provincial_end_date = us_co2_map.loc[relevant_time, 'date'].max()

    ## 1.1 apply a filter to select us(region) data only (during the above period when us data is available)
    filt = ((us_co2_map['date'] >= us_provincial_start_date) &
            (us_co2_map['date'] <= us_provincial_end_date))
    sub_us_co2_map = us_co2_map.loc[filt]

    ## 1.2 calculate weighted share of for each day:
    ## Share of Coal (country-level) = Sum of all provinces (Share of Coal * btc miner share std)
    cols = ['Share of Coal', 'Share of Gas',
            'Share of Hydro', 'Share of Solar', 'Share of Wind', 'Share of Oil',
            'Share of Nuclear', 'Share of Other renewable']
    for col in cols:
        sub_us_co2_map['weighted' + ' ' + col] = sub_us_co2_map['btc miner share std'] * sub_us_co2_map[col]

    to_merge_us_co2_map = sub_us_co2_map.groupby(['date', 'region'], as_index=False)['weighted Share of Coal',
    'weighted Share of Gas',
    'weighted Share of Hydro', 'weighted Share of Solar',
    'weighted Share of Wind', 'weighted Share of Oil',
    'weighted Share of Nuclear', 'weighted Share of Other renewable'].sum()

    to_merge_us_co2_map.columns = ['date', 'country/state', 'Share of Coal', 'Share of Gas',
                                   'Share of Hydro', 'Share of Solar', 'Share of Wind', 'Share of Oil',
                                   'Share of Nuclear', 'Share of Other renewable']
    to_merge_us_co2_map['country/state'] = 'United States'

    ## 1.3 apply a filter of same data period to select rows to change
    filt = ((eth_miner_share_total_filled['date'] >= us_provincial_start_date) &
            (eth_miner_share_total_filled['date'] <= us_provincial_end_date) &
            (eth_miner_share_total_filled['country/state'] == 'United States'))
    to_change_us_df = eth_miner_share_total_filled.loc[filt]
    eth_miner_share_total_filled = eth_miner_share_total_filled.drop(to_change_us_df.index)  ##exclude the selected rows
    to_change_us_df.drop(cols, axis=1, inplace=True)

    ## 1.4 merge two dataframes together
    to_change_us_df = pd.merge(to_change_us_df, to_merge_us_co2_map, how='left',
                               on=['date', 'country/state'])

    ## 1.5 append back total dataframe and sort
    eth_miner_share_total_filled = eth_miner_share_total_filled.append(to_change_us_df)
    eth_miner_share_total_filled = eth_miner_share_total_filled.sort_values(['date', 'region'])
    return eth_miner_share_total_filled


def calculate_weighted_country_share_within_region(eth_miner_share_total_filled, us_generation_mix, region_mix):
    '''
    this function is used to calculate weighted share of miner distribution of each country/state in each region
    for historical period and prediction period
    '''
    eth_miner_share_total_filled['CO2_Coef'] = (eth_miner_share_total_filled['Share of Coal'] * co2_g_p_kwh_coal +
                                                eth_miner_share_total_filled['Share of Gas'] * co2_g_p_kwh_gas +
                                                eth_miner_share_total_filled['Share of Oil'] * co2_g_p_kwh_oil +
                                                eth_miner_share_total_filled['Share of Nuclear'] * co2_g_p_kwh_nuc +
                                                eth_miner_share_total_filled['Share of Hydro'] * co2_g_p_kwh_hydro +
                                                eth_miner_share_total_filled['Share of Wind'] * co2_g_p_kwh_wind +
                                                eth_miner_share_total_filled['Share of Solar'] * co2_g_p_kwh_solar +
                                                eth_miner_share_total_filled[
                                                    'Share of Other renewable'] * co2_g_p_kwh_oRenew)
    # historical - electricity generation weight within each region group
    eth_miner_share_total_filled['region_group_count'] = eth_miner_share_total_filled.groupby(
        ['date', 'region'])['country/state'].transform('count')
    eth_miner_share_total_filled = pd.merge(eth_miner_share_total_filled, region_mix, how='left', on=['Code', 'Year'])
    filt = (eth_miner_share_total_filled['Code'] == 'OWID_WRL') & (
        eth_miner_share_total_filled['country/state'] != 'Others')
    eth_miner_share_total_filled.loc[filt, 'Total Electricity (TWh)'] = 0

    # historical - electricity generation weight for US state level
    eth_miner_share_total_filled = pd.merge(eth_miner_share_total_filled, us_generation_mix,
                                            how='left', on=['Year', 'province_code'])
    filt = pd.notnull(eth_miner_share_total_filled['GENERATION (Megawatthours)'])
    eth_miner_share_total_filled.loc[filt, 'Total Electricity (TWh)'] = eth_miner_share_total_filled.loc[
        filt, 'GENERATION (Megawatthours)']
    eth_miner_share_total_filled['Region Total Electricity (TWh)'] = eth_miner_share_total_filled.groupby(
        ['date', 'region'])['Total Electricity (TWh)'].transform('sum')
    eth_miner_share_total_filled['Region Electricity Share'] = eth_miner_share_total_filled['Total Electricity (TWh)'] / \
                                                               eth_miner_share_total_filled[
                                                                   'Region Total Electricity (TWh)']
    his_filt = eth_miner_share_total_filled['date'] < map_start_date
    eth_miner_share_total_filled.loc[his_filt, 'btc miner share std'] = eth_miner_share_total_filled.loc[
        his_filt, 'Region Electricity Share']

    # assessment period - fill empty 0 value

    eth_miner_share_total_filled = eth_miner_share_total_filled.sort_values(['country/state', 'region', 'date'])
    future_filt_us = (eth_miner_share_total_filled['date'] < us_map_start_date) & (
        eth_miner_share_total_filled['date'] >= map_start_date) & (eth_miner_share_total_filled['Code'] == 'USA')

    eth_miner_share_total_filled.loc[future_filt_us, 'btc miner share std'] = eth_miner_share_total_filled.loc[
        future_filt_us, 'Region Electricity Share']
    eth_miner_share_total_filled.drop(['region_group_count'], axis=1, inplace=True)
    filt = eth_miner_share_total_filled['region'] == 'others'
    eth_miner_share_total_filled.loc[filt, 'btc miner share std'] = 1

    # future forward fill
    # fill China and US states-level data (using the last avaialbe date)
    forward_fill_filt = (eth_miner_share_total_filled['level'] == 'states') & (
        eth_miner_share_total_filled['date'] >= map_start_date)
    eth_miner_share_total_filled.loc[forward_fill_filt, 'btc miner share std'] = eth_miner_share_total_filled.loc[
        forward_fill_filt, 'btc miner share std'].ffill()

    # fill mining map data period na value with 0
    current_filt = (eth_miner_share_total_filled['date'] >= map_start_date) & (
        eth_miner_share_total_filled['date'] <= map_end_date) & (
                       eth_miner_share_total_filled['level'] == 'countries')
    eth_miner_share_total_filled.loc[current_filt, 'btc miner share std'] = eth_miner_share_total_filled.loc[
        current_filt, 'btc miner share std'].fillna(0)

    # forward fill
    eth_miner_share_total_filled['btc miner share std'] = eth_miner_share_total_filled['btc miner share std'].ffill()
    eth_miner_share_total_filled['intensity'] = eth_miner_share_total_filled['miner distribution share'] * \
                                                eth_miner_share_total_filled['btc miner share std'] * \
                                                eth_miner_share_total_filled['CO2_Coef']

    return eth_miner_share_total_filled


def calculate_co2_emission(co2_intensity, df2):
    '''
    this function is used to calculate global co2 emissions
    '''
    df2['Date and Time'] = pd.to_datetime(df2['Date and Time'])
    df2['monthly date'] = df2['Date and Time'].astype("str")

    graph_data = pd.merge(df2, co2_intensity, how='left', on='monthly date')

    graph_data['MAX'] = graph_data['annualised consumption MAX, TWh'].astype(float)
    graph_data['MIN'] = graph_data['annualised consumption MIN, TWh'].astype(float)
    graph_data['GUESS'] = graph_data['annualised consumption GUESS, TWh'].astype(float)

    graph_data['MAX_CO2'] = (graph_data['GUESS'] * 10 ** 9 * co2_g_p_kwh_coal) / 1000000000000
    # 10**6 = conversion to tons and 10**9 # conversion from twh to kwh

    graph_data['MIN_CO2'] = (graph_data['GUESS'] * 10 ** 9 * co2_g_p_kwh_hydro) / 1000000000000
    # 10**6 = conversion to tons and 10**9 # conversion from twh to kwh

    graph_data['GUESS_CO2'] = (graph_data['GUESS'] * 10 ** 9 * graph_data['intensity']) / 1000000000000
    # 10**6 = conversion to tons and 10**9 # conversion from twh to kwh
    graph_data.drop('monthly date', axis=1, inplace=True)
    return graph_data
