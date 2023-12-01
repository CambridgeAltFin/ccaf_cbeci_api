import click
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import statistics as stats
from os.path import join
import os
import time
import datetime

from config import config
from components.gas_emission.gas_emission_factory import EmissionIntensityServiceFactory


map_end_date = '2021-09-01'
map_start_date = '2019-09-01'

co2_g_p_kwh_hydro = 24  # [2]
co2_g_p_kwh_wind = np.mean([11, 12])  # onshore & offshore [2]
co2_g_p_kwh_nuc = 12  # [2]
co2_g_p_kwh_solar = np.mean([41, 48, 27])  # solar rooftop, solar utility, concentrated solar power [2]
co2_g_p_kwh_gas = 469  # Natural Gas [2]
co2_g_p_kwh_coal = 820  # [2]
co2_g_p_kwh_oil = 840  # [1]
co2_g_p_kwh_oRenew = np.mean([38, 740, 230])  # geothermal, Biomass (cofiring) & Biomass (dedicated) [2]

emission_intensity_service = EmissionIntensityServiceFactory.create()

monthly_globalw_CO2_Coeff = {}


@click.command(name='data:calc:co2-coefficient')
@click.argument('directory')
def handle(directory):
    df3 = get_electricity_mix(join(directory, 'per-capita-electricity-source-stacked.csv'))

    with psycopg2.connect(**config['custom_data']) as connection:
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        df = get_hashrate_share(cursor)
        df2, historical_elec, post_elec = get_electricity_estimates(cursor)
        china_coefficient, china_powermix = get_china_map(cursor, df, df3, directory)

        his_graph_data = historical_co2_coefficient(historical_elec, df3)
        # save_co2_coefficient(his_graph_data, emission_intensity_service.HISTORICAL)

        df3 = df3.replace({'Entity': {'Iran': 'Iran, Islamic Rep.', 'Russia': 'Russian Federation', 'World': 'Other',
                                      'China': 'Mainland China', 'Germany': 'Germany *', 'Ireland': 'Ireland *'}})

        df4 = estimated_co2_coefficient(df, df2, df3, china_coefficient)
        # save_co2_coefficient(df4, emission_intensity_service.ESTIMATED)

        df5 = provisional_co2_coefficient(post_elec)
        # save_co2_coefficient(df5, emission_intensity_service.PROVISIONAL)

        energy_mix_to_merge = df3[['Entity', 'Year', 'Share of Coal', 'Share of Gas', 'Share of Oil',
                                   'Share of Nuclear', 'Share of Hydro', 'Share of Wind', 'Share of Solar',
                                   'Share of Other renewable']]
        hashrate = df.copy()
        hashrate['Year'] = hashrate['Date'].str.extract(r'^(\d{4})')
        hashrate['Year'] = hashrate['Year'].astype('int')

        china_powermix['Weighted Share of Coal'] = china_powermix['Share of Coal'] * china_powermix[
            'Percentage Share of Chinese hashrate']
        china_powermix['Weighted Share of Nuclear'] = china_powermix['Share of Nuclear'] * china_powermix[
            'Percentage Share of Chinese hashrate']
        china_powermix['Weighted Share of Hydro'] = china_powermix['Share of Hydro'] * china_powermix[
            'Percentage Share of Chinese hashrate']
        china_powermix['Weighted Share of Wind'] = china_powermix['Share of Wind'] * china_powermix[
            'Percentage Share of Chinese hashrate']
        china_powermix['Weighted Share of Solar'] = china_powermix['Share of Solar'] * china_powermix[
            'Percentage Share of Chinese hashrate']

        china_powermix_monthly = china_powermix.groupby(['Year', 'Month'], as_index=False)[
            ['Weighted Share of Coal', 'Weighted Share of Nuclear',
             'Weighted Share of Hydro',
             'Weighted Share of Wind',
             'Weighted Share of Solar']].sum()
        china_powermix_monthly['merge_date'] = pd.to_datetime(
            china_powermix_monthly['Year'].astype('str') + '-' + china_powermix_monthly['Month'].astype('str') + '-01')
        china_powermix_monthly.drop(['Year', 'Month'], axis=1, inplace=True)

        hashrate_merge = pd.merge(hashrate, energy_mix_to_merge, how='left',
                                  left_on=['Year', 'Country Equivalent'], right_on=['Year', 'Entity'])
        hashrate_merge = hashrate_merge.sort_values(['Country Equivalent', 'Date'])
        hashrate_merge = hashrate_merge.reset_index(drop=True)
        hashrate_merge = hashrate_merge.fillna(method='ffill')

        hashrate_merge_china = hashrate_merge[hashrate_merge['Country'] == 'Mainland China']
        hashrate_merge_china = hashrate_merge_china.iloc[:, 0:9]
        hashrate_merge_china['merge_date'] = pd.to_datetime(hashrate_merge_china['Date'] + '-01')
        hashrate_merge_china = pd.merge(hashrate_merge_china, china_powermix_monthly, how='left', on='merge_date')

        hashrate_merge_china['Entity'] = hashrate_merge_china['Country']
        hashrate_merge_china.drop('merge_date', axis=1, inplace=True)
        hashrate_merge_china.drop('Share of Coal', axis=1, inplace=True)
        hashrate_merge_china.rename({
            'Weighted Share of Coal': 'Share of Coal',
            'Weighted Share of Nuclear': 'Share of Nuclear',
            'Weighted Share of Hydro': 'Share of Hydro',
            'Weighted Share of Wind': 'Share of Wind',
            'Weighted Share of Solar': 'Share of Solar'
        }, axis=1, inplace=True)

        hashrate_merge = hashrate_merge[hashrate_merge['Country'] != 'Mainland China']
        hashrate_merge = hashrate_merge.append(hashrate_merge_china, ignore_index=True)
        hashrate_merge = hashrate_merge.fillna(0)

        power_source_share = ['Share of Coal', 'Share of Gas', 'Share of Oil',
                              'Share of Nuclear', 'Share of Hydro', 'Share of Wind', 'Share of Solar',
                              'Share of Other renewable']
        for power in power_source_share:
            hashrate_merge["Weighted " + power] = hashrate_merge['Share of global hashrate'] / 100 * hashrate_merge[
                power]

        weighted_source_share = []
        for power in power_source_share:
            weighted_source_share.append("Weighted " + power)

        monthly_bitcoin_energy_mix = hashrate_merge.groupby('Date')[weighted_source_share].sum()
        monthly_bitcoin_energy_mix.sum(axis=1)
        monthly_bitcoin_energy_mix = monthly_bitcoin_energy_mix.reset_index(drop=False)
        monthly_bitcoin_energy_mix['Date'] = pd.to_datetime(monthly_bitcoin_energy_mix['Date'] + '-01')
        monthly_bitcoin_energy_mix['Year'] = monthly_bitcoin_energy_mix['Date'].dt.year
        yearly_bitcoin_energy_mix = monthly_bitcoin_energy_mix.groupby('Year')[weighted_source_share].mean()
        yearly_bitcoin_energy_mix.sum(axis=1)

        g_monthly_bitcoin_energy_mix = pd.melt(monthly_bitcoin_energy_mix, id_vars='Date', value_vars=weighted_source_share,
                                               var_name='Power Source', value_name='Power Source Share')
        g_monthly_bitcoin_energy_mix['Power Source'] = g_monthly_bitcoin_energy_mix['Power Source'].str.extract(
            'Weighted Share of (.+)')

        for i, row in g_monthly_bitcoin_energy_mix.iterrows():
            cursor.execute('insert into power_sources (type, name, value, timestamp, date, asset) VALUES (%s, %s, %s, %s, %s, %s)',
                           ('monthly', row['Power Source'], round(row['Power Source Share'], 6), row['Date'].timestamp(), row['Date'], 'btc'))

        yearly_bitcoin_energy_mix = yearly_bitcoin_energy_mix.reset_index(drop=False)
        graph_bitcoin_energy_mix = pd.melt(yearly_bitcoin_energy_mix, id_vars='Year', value_vars=weighted_source_share,
                                           var_name='Power Source', value_name='Power Source Share')
        ## reformat
        graph_bitcoin_energy_mix['Power Source'] = graph_bitcoin_energy_mix['Power Source'].str.extract(
            'Weighted Share of (.+)')
        graph_bitcoin_energy_mix['Year'] = pd.to_datetime(graph_bitcoin_energy_mix['Year'].astype('str') + '-01-01')
        graph_bitcoin_energy_mix = graph_bitcoin_energy_mix.sort_values(['Year', 'Power Source Share'],
                                                                        ascending=[True, False])
        graph_bitcoin_energy_mix['Power Source Share'] = round(graph_bitcoin_energy_mix['Power Source Share'] * 100, 2)

        for i, row in graph_bitcoin_energy_mix.iterrows():
            cursor.execute('insert into power_sources (type, name, value, timestamp, date, asset) VALUES (%s, %s, %s, %s, %s, %s)',
                           ('yearly', row['Power Source'], round(row['Power Source Share'], 6), row['Year'].timestamp(), row['Year'], 'btc'))


def hashrate_filter(country_name, manual_adjust_country_list):
    """
    This function takes filters for country
    with above average electricity cost

    Input:
    country_name = str

    Output:
    Boolean
    """

    if country_name in manual_adjust_country_list:
        return 'Other'
    else:
        return country_name


def get_china_map(cursor, df, df3, directory):
    prov_season_table = get_prov(join(directory, 'Engergy Consumption_China_2019-2021_By Province.xlsx'))

    sql = 'select m.date "Date", m.name "Province", m.local_value * 100 "Share of Chinese hashrate" from mining_area_provinces m where m.version = %s'
    cursor.execute(sql, ('1.1.1',))
    china_map_data = cursor.fetchall()
    china_map = pd.DataFrame(china_map_data)
    china_map['Date'] = pd.to_datetime(china_map['Date'])

    manual_adjust_country_list = ['Germany *', 'Ireland *']
    df['Country Equivalent'] = df['Country'].apply(lambda x: hashrate_filter(x, manual_adjust_country_list))

    china_powermix = pd.merge(china_map, prov_season_table, how='left', left_on=['Date', 'Province'],
                              right_on=['Date and Time', 'Province'])
    china_powermix.drop('Date and Time', axis=1, inplace=True)
    china_powermix['Year'] = china_powermix['Date'].dt.year

    corresponding_col = {'Hydro': 'Hydro electricity per capita (kWh)',
                         'Nuclear': 'Nuclear electricity per capita (kWh)',
                         'Solar': 'Solar electricity per capita (kWh)',
                         'Thermal': 'Coal electricity per capita (kWh)',
                         'Wind': 'Wind electricity per capita (kWh)'}
    for index, row in china_powermix.iterrows():
        for col in corresponding_col.keys():
            if pd.isnull(row[col]):
                year = row['Year']
                country = 'China'
                x = df3[(df3['Entity'] == 'China') & (df3['Year'] == row['Year'])][corresponding_col[col]]

                if not isinstance(x, np.float64):
                    year -= 1
                    x = df3[(df3['Entity'] == country) & (df3['Year'] == year)][corresponding_col[col]]

                    if not isinstance(x, np.float64):
                        year -= 1
                        x = df3[(df3['Entity'] == country) & (df3['Year'] == year)][corresponding_col[col]]

                value = float(x)
                china_powermix.at[index, col] = value
    china_powermix['Total'] = china_powermix['Hydro'] + china_powermix['Nuclear'] + china_powermix['Solar'] + \
                              china_powermix['Thermal'] + china_powermix['Wind']
    china_powermix['Share of Coal'] = china_powermix['Thermal'] / china_powermix['Total']
    china_powermix['Share of Nuclear'] = china_powermix['Nuclear'] / china_powermix['Total']
    china_powermix['Share of Hydro'] = china_powermix['Hydro'] / china_powermix['Total']
    china_powermix['Share of Wind'] = china_powermix['Wind'] / china_powermix['Total']
    china_powermix['Share of Solar'] = china_powermix['Solar'] / china_powermix['Total']

    china_powermix['CO2_Coef'] = (china_powermix['Share of Coal'] * co2_g_p_kwh_coal +
                                  china_powermix['Share of Nuclear'] * co2_g_p_kwh_nuc +
                                  china_powermix['Share of Hydro'] * co2_g_p_kwh_hydro +
                                  china_powermix['Share of Wind'] * co2_g_p_kwh_wind +
                                  china_powermix['Share of Solar'] * co2_g_p_kwh_solar)
    china_powermix['Percentage Share of Chinese hashrate'] = china_powermix['Share of Chinese hashrate'] / 100
    china_powermix['Weighted_CO2_Coef'] = china_powermix['CO2_Coef'] * china_powermix[
        'Percentage Share of Chinese hashrate']
    china_powermix['Month'] = china_powermix['Date'].dt.month

    china_coefficient = china_powermix.groupby(['Year', 'Month'], as_index=False)['Weighted_CO2_Coef'].sum()
    china_coefficient['Date'] = pd.to_datetime(
        china_coefficient['Year'].astype('str') + "-" + china_coefficient['Month'].astype('str') + '-01')
    china_coefficient['Country'] = 'Mainland China'
    china_coefficient.rename({'Weighted_CO2_Coef': 'CO2_Coef'}, axis=1, inplace=True)
    china_coefficient = china_coefficient[['Date', 'CO2_Coef', 'Country']]
    return china_coefficient, china_powermix


def get_hashrate_share(cursor):
    sql = 'select m.date "Date", m.name "Country", m.value * 100 "Share of global hashrate" from mining_area_countries m where m.version = %s'
    cursor.execute(sql, ('1.1.1',))
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    df['Share of global hashrate'] = round(df['Share of global hashrate'], 2)
    return df


def get_electricity_mix(path):
    df3 = pd.read_csv(path)

    df3['Sum'] = df3['Coal electricity per capita (kWh)'] + df3['Gas electricity per capita (kWh)'] + df3[
        'Oil electricity per capita (kWh)'] + df3['Nuclear electricity per capita (kWh)'] + df3[
                     'Hydro electricity per capita (kWh)'] + df3['Wind electricity per capita (kWh)'] + df3[
                     'Solar electricity per capita (kWh)'] + df3['Other renewable electricity per capita (kWh)']
    # Share of respective electricity
    df3['Share of Coal'] = df3['Coal electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Gas'] = df3['Gas electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Oil'] = df3['Oil electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Nuclear'] = df3['Nuclear electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Hydro'] = df3['Hydro electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Wind'] = df3['Wind electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Solar'] = df3['Solar electricity per capita (kWh)'] / df3['Sum']
    df3['Share of Other renewable'] = df3['Other renewable electricity per capita (kWh)'] / df3['Sum']

    df3['CO2_Coef'] = (df3['Share of Coal'] * co2_g_p_kwh_coal +
                       df3['Share of Gas'] * co2_g_p_kwh_gas +
                       df3['Share of Oil'] * co2_g_p_kwh_oil +
                       df3['Share of Nuclear'] * co2_g_p_kwh_nuc +
                       df3['Share of Hydro'] * co2_g_p_kwh_hydro +
                       df3['Share of Wind'] * co2_g_p_kwh_wind +
                       df3['Share of Solar'] * co2_g_p_kwh_solar +
                       df3['Share of Other renewable'] * co2_g_p_kwh_oRenew)

    return df3


def get_electricity_estimates(cursor):
    sql = 'select e.timestamp "Timestamp", e.date "Date and Time", e.max_power "power MAX, GW", e.min_power "power MIN, GW", e.guess_power "power GUESS, GW", e.max_consumption "annualised consumption MAX, TWh", e.min_consumption "annualised consumption MIN, TWh", e.guess_consumption "annualised consumption GUESS, TWh" from consumptions e where e.asset = \'btc\' and e.price = 5'
    cursor.execute(sql)
    data = cursor.fetchall()
    df2 = pd.DataFrame(data)
    df2['date'] = pd.to_datetime(df2['Date and Time'])
    historical_elec = df2[df2['date'] < map_start_date]
    post_elec = df2[df2['date'] >= map_end_date]
    df2 = df2[(df2['date'] >= map_start_date) & (df2['date'] < map_end_date)]
    df2.drop('date', axis=1, inplace=True)
    df2 = df2.reset_index(drop=True)
    return df2, historical_elec, post_elec


def get_cumulative_electricity_consumed(cursor):
    sql = 'select c.date "Month", c.monthly_consumption "Monthly consumption, TWh", c.cumulative_consumption "Cumulative consumption, TWh" from cumulative_energy_consumptions c where c.price = \'0.05\''
    cursor.execute(sql)
    ele_cum = pd.DataFrame(cursor.fetchall())
    ele_cum['Month'] = pd.to_datetime(ele_cum['Month'])
    return ele_cum


def get_prov(path):
    prov = pd.read_excel(path)

    prov_wide = pd.pivot(prov, index=['Season', 'Province', 'Year'],
                         columns=['Electricity Consumption'],
                         values=['Amount (Unit: Billion kWh)'])['Amount (Unit: Billion kWh)'].reset_index(drop=False)
    prov_wide.columns.name = None
    prov_wide['Year'] = prov_wide['Year'].astype('str')
    prov_wide['Year'] = prov_wide['Year'].str.extract(r'(\d{4}$)')
    prov_wide['Year'] = prov_wide['Year'].astype('int')
    prov_wide['Province'] = prov_wide['Province'].replace({'Inner Mongolia': 'Nei Mongol'})
    prov_wide.drop('Season', axis=1, inplace=True)

    timetable = pd.DataFrame(pd.date_range(start=map_start_date, end=map_end_date, freq='1M'), columns=['End of Month'])
    timetable['Date and Time'] = timetable['End of Month'].dt.year.astype('str') + '-' + timetable[
        'End of Month'].dt.month.astype('str') + '-01'
    timetable['Date and Time'] = pd.to_datetime(timetable['Date and Time'])
    # timetable['Month'] = timetable['Date and Time'].dt.month
    timetable['Year'] = timetable['Date and Time'].dt.year
    timetable = timetable[['Date and Time', 'Year']]

    prov_season_table = pd.merge(timetable, prov_wide, how='left', on=['Year'])
    return prov_season_table


def historical_co2_coefficient(historical_elec, df3):
    his_energy_mix = df3[df3['Entity'] == 'World'].reset_index()

    his_co2 = his_energy_mix[['Year', 'CO2_Coef']]

    historical_elec['Date and Time'] = pd.to_datetime(historical_elec['Date and Time'])
    historical_elec['Year'] = historical_elec['Date and Time'].dt.year
    his_graph_data = pd.merge(historical_elec, his_co2, how='left', on='Year')
    his_graph_data.rename({'CO2_Coef': 'Global hashrate-weighted CO2 Coefficient'}, axis=1, inplace=True)
    return his_graph_data


def estimated_co2_coefficient(df, df2, df3, china_coefficient):
    df['CO2_Coef'] = 0

    for i in range(len(df)):

        # select search parameter:
        # year = np.int64(df['Date'][i].split('/')[-1])
        df['Date'] = pd.to_datetime(df['Date'])
        year = df['Date'].dt.year[i]
        country = str(df['Country Equivalent'][i])

        # search & select 'CO2_Coef'
        x = df3[(df3['Entity'] == country) & (df3['Year'] == year)]['CO2_Coef']

        # if data value not availble select last available value
        if x.empty:

            year -= 1
            x = df3[(df3['Entity'] == country) & (df3['Year'] == year)]['CO2_Coef']

            if x.empty:
                year -= 1
                x = df3[(df3['Entity'] == country) & (df3['Year'] == year)]['CO2_Coef']

                # convert to float
        coef = float(x)

        # assign
        df.at[i, 'CO2_Coef'] = coef

    for index, row in df.iterrows():
        if row['Country'] == 'Mainland China':
            value = china_coefficient[(china_coefficient['Date'] == row['Date'])]['CO2_Coef'].to_list()
            if len(value) == 0:
                value = [0]
            df.at[index, 'CO2_Coef'] = float(value[0])
    df['Weighted_CO2_Coef'] = (df['Share of global hashrate'] / 100) * df['CO2_Coef']

    df['Date'] = pd.to_datetime(df['Date'])
    df['Date'] = df['Date'].dt.strftime('%Y-%m')
    for i in list(df['Date'].unique()):
        wg_co2 = float(df[df['Date'] == i]['Weighted_CO2_Coef'].sum())

        # name= i.split('/')[2:0:-1]

        # monthly_globalw_CO2_Coeff['-'.join(name)] = wg_co2

        monthly_globalw_CO2_Coeff[i] = wg_co2
    pd.DataFrame.from_dict(monthly_globalw_CO2_Coeff, orient='index',
                           columns=['Global Hashrate-weighted CO2 Coefficent(gCO2eq/kWh)'])

    df2['Global hashrate-weighted CO2 Coefficient'] = 0
    df2['Date and Time'] = pd.to_datetime(df2['Date and Time'])
    df2['Date'] = df2['Date and Time'].dt.strftime('%Y-%m')

    for i in range(0, len(df2)):

        # derive dictionary key for look-up from date
        key = df2['Date'][i]

        # look up & assign
        try:
            df2['Global hashrate-weighted CO2 Coefficient'][i] = monthly_globalw_CO2_Coeff[key]
        except KeyError:
            pass
    return df2[['Timestamp', 'Date and Time', 'Global hashrate-weighted CO2 Coefficient']]


def provisional_co2_coefficient(post_elec):
    monthly_globalw_CO2_Coeff_df = pd.DataFrame.from_dict(monthly_globalw_CO2_Coeff, orient='index', columns=[
        'Global Hashrate-weighted CO2 Coefficent(gCO2eq/kWh)'])
    monthly_globalw_CO2_Coeff_df.reset_index(drop=False, inplace=True)
    monthly_globalw_CO2_Coeff_df = monthly_globalw_CO2_Coeff_df.sort_values('index', ascending=False)
    monthly_globalw_CO2_Coeff_df.reset_index(drop=True, inplace=True)
    newest_co2_coefficient = monthly_globalw_CO2_Coeff_df.loc[
        0, 'Global Hashrate-weighted CO2 Coefficent(gCO2eq/kWh)']
    post_elec['Global hashrate-weighted CO2 Coefficient'] = newest_co2_coefficient
    return post_elec[['Timestamp', 'Date and Time', 'Global hashrate-weighted CO2 Coefficient']]


def save_co2_coefficient(his_co2, name):
    for i, row in his_co2.iterrows():
        emission_intensity_service.create_chart_point(
            row['Date and Time'],
            round(row['Global hashrate-weighted CO2 Coefficient'], 6),
            name
        )
