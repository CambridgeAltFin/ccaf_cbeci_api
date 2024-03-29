from flask import Blueprint, request
from datetime import datetime

from components.gas_emission import GreenhouseGasEmissionServiceFactory, EmissionIntensityServiceFactory, \
    PowerMixServiceFactory, EmissionServiceFactory
from services import EnergyConsumption, EnergyConsumptionByTypes
from components.energy_consumption.v1_3_1 import EnergyConsumptionServiceFactory as EnergyConsumptionServiceFactory_v1_3_1
from components.energy_consumption import EnergyConsumptionServiceFactory
from components.energy_consumption.v1_1_1 import \
    EnergyConsumptionServiceFactory as EnergyConsumptionServiceFactory_v1_1_1
from queries import get_mining_countries, get_mining_provinces
from packaging.version import parse as version_parse
from calendar import month_name
from decorators.cache_control import cache_control
from helpers import send_file
from components.bitcoin_cost_of_minting import BitcoinCostOfMintingServiceFactory

bp = Blueprint('download', __name__, url_prefix='/download')


def get_data(version=None, price=0.05):
    def to_dict(timestamp, row):
        return {
            'timestamp': timestamp,
            'date': datetime.utcfromtimestamp(timestamp).isoformat(),
            'guess_consumption': row['guess_consumption'],
            'max_consumption': row['max_consumption'],
            'min_consumption': row['min_consumption']
        }

    def v1_0_5(price):
        energy_consumption = EnergyConsumption()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    def v1_1_0(price):
        energy_consumption_by_types = EnergyConsumptionByTypes()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption_by_types.get_data(price)]

    def v1_1_1(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_consumption = EnergyConsumptionServiceFactory_v1_1_1.create()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    def v1_2_0(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_consumption = EnergyConsumptionServiceFactory_v1_3_1.create()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    def v1_4_0(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_consumption = EnergyConsumptionServiceFactory.create()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    def v1_5_0(price):
        def to_dict(timestamp, row):
            return {
                'timestamp': timestamp,
                'date': datetime.utcfromtimestamp(timestamp).isoformat(),
                'guess_consumption': row['guess_consumption'],
                'max_consumption': row['max_consumption'],
                'min_consumption': row['min_consumption'],
                'guess_power': row['guess_power'],
                'max_power': row['max_power'],
                'min_power': row['min_power']
            }

        energy_consumption = EnergyConsumptionServiceFactory.create()

        return [to_dict(timestamp, row) for timestamp, row in energy_consumption.get_data(price)]

    func = locals().get(version.replace('.', '_'))
    if callable(func):
        return func(price)

    raise NotImplementedError('Not Implemented')


def get_monthly_data(version, price=.05):
    def to_dict(date, row):
        return {
            'month': month_name[date.month] + date.strftime(' %Y'),
            'value': round(row['guess_consumption'], 4),
            'cumulative_value': round(row['cumulative_guess_consumption'], 4),
        }

    if version == 'v1.1.1':
        energy_consumption = EnergyConsumptionServiceFactory_v1_1_1.create()
    elif version == 'v1.2.0' or version == 'v1.3.1':
        energy_consumption = EnergyConsumptionServiceFactory_v1_3_1.create()
    elif version == 'v1.4.0' or version == 'v1.5.0':
        energy_consumption = EnergyConsumptionServiceFactory.create()
    else:
        raise NotImplementedError('Not Implemented')

    return [to_dict(date, row) for date, row in energy_consumption.get_monthly_data(price)]


def get_yearly_data(version, price=.05):
    def to_dict(date, row):
        return {
            'year': date.strftime('%Y'),
            'value': round(row['guess_consumption'], 4),
            'cumulative_value': round(row['cumulative_guess_consumption'], 4),
        }

    if version == 'v1.1.1':
        energy_consumption = EnergyConsumptionServiceFactory_v1_1_1.create()
    elif version == 'v1.2.0' or version == 'v1.3.1':
        energy_consumption = EnergyConsumptionServiceFactory_v1_3_1.create()
    elif version == 'v1.4.0' or version == 'v1.5.0':
        energy_consumption = EnergyConsumptionServiceFactory.create()
    else:
        raise NotImplementedError('Not Implemented')

    return [to_dict(date, row) for date, row in energy_consumption.get_yearly_data(price)]


@bp.route('/data')
@cache_control()
def data(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)
    headers = {
        'timestamp': 'Timestamp',
        'date': 'Date and Time',
        'max_consumption': 'MAX',
        'min_consumption': 'MIN',
        'guess_consumption': 'GUESS'
    }

    if version_parse(version) >= version_parse('v1.1.1'):
        headers = {
            'timestamp': 'Timestamp',
            'date': 'Date and Time',
            'max_power': 'power MAX, GW',
            'min_power': 'power MIN, GW',
            'guess_power': 'power GUESS, GW',
            'max_consumption': 'annualised consumption MAX, TWh',
            'min_consumption': 'annualised consumption MIN, TWh',
            'guess_consumption': 'annualised consumption GUESS, TWh'
        }

    rows = get_data(version, float(price))
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh', file_type=file_type)

    return send_file_func(headers, rows)


@bp.route('/data/monthly')
@cache_control()
def data_monthly(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)

    headers = {
        'month': 'Month',
        'value': 'Monthly consumption, TWh',
        'cumulative_value': 'Cumulative consumption, TWh',
    }

    rows = get_monthly_data(version, float(price))
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh', file_type=file_type)

    return send_file_func(headers, rows)


@bp.route('/data/yearly')
@cache_control()
def data_yearly(version=None):
    file_type = request.args.get('file_type', 'csv')
    price = request.args.get('price', 0.05)

    headers = {
        'year': 'Year',
        'value': 'Yearly consumption, TWh',
        'cumulative_value': 'Cumulative consumption, TWh',
    }

    rows = get_yearly_data(version, float(price))
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh', file_type=file_type)

    return send_file_func(headers, rows)


@bp.route('/profitability_equipment')
@cache_control()
def profitability_equipment(version=None):
    price = request.args.get('price', 0.05)

    send_file_func = send_file()

    headers = {
        'timestamp': 'Timestamp',
        'date': 'Date',
        'profitability_equipment': 'Profitability equipment',
        'equipment_list': 'Equipment list',
    }

    def miner_to_str(miner):
        miner_str = miner['miner_name']
        if miner['type']:
            miner_str += ' - ({type})'.format(type=miner['type'])
        return miner_str

    def to_dict(row):
        return {
            'timestamp': row['timestamp'],
            'date': row['date'],
            'profitability_equipment': row['profitability_equipment'] * 1000,
            'equipment_list': '; '.join(list(map(miner_to_str, row['equipment_list']))),
        }

    if version == 'v1.0.5':
        energy_consumption = EnergyConsumption()
    elif version == 'v1.1.0':
        energy_consumption = EnergyConsumptionByTypes()
    elif version == 'v1.1.1':
        energy_consumption = EnergyConsumptionServiceFactory_v1_1_1.create()
    elif version == 'v1.2.0':
        energy_consumption = EnergyConsumptionServiceFactory_v1_3_1.create()
    elif version == 'v1.4.0' or version == 'v1.5.0':
        energy_consumption = EnergyConsumptionServiceFactory.create()
    else:
        raise NotImplementedError('Not Implemented')

    rows = [to_dict(row) for row in energy_consumption.get_equipment_list(float(price))]

    return send_file_func(headers, rows, filename='profitability_equipment.csv')


@bp.route('/mining_countries')
@cache_control()
def mining_countries(version=None):
    if version not in ['v1.1.0', 'v1.1.1', 'v1.2.0', 'v1.5.0']:
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')

    rows = get_mining_countries(version[1:])
    send_file_func = send_file(file_type=file_type)

    if version == 'v1.1.0':
        headers = {
            'date': 'Date',
            'name': 'Country',
            'value': 'Share of global hashrate',
        }

        return send_file_func(headers, list(map(lambda row: {**row, 'value': round(row['value'] * 100, 2)}, rows)))

    headers = {
        'date': 'date',
        'name': 'country',
        'value': 'monthly_hashrate_%',
        'absolute_value': 'monthly_absolute_hashrate_EH/S',
    }

    return send_file_func(
        headers,
        list(map(lambda row: {
            **row,
            'value': str(round(row['value'] * 100, 2)) + '%' if row['value'] else '0%',
            'absolute_value': round(row['absolute_value'], 2) if row['absolute_value'] is not None else 0
        }, rows))
    )


@bp.route('/mining_provinces')
@bp.route('/mining_provinces/<country>')
@cache_control()
def mining_provinces(version=None, country='cn'):
    country_titles = {
        'cn': {
            'name': 'Province',
            'local_value': 'Share of Chinese hashrate'
        },
        'us': {
            'name': 'State',
            'local_value': 'US hashrate by state'
        },
    }

    if version not in ['v1.1.0', 'v1.1.1', 'v1.2.0', 'v1.5.0'] or country not in country_titles:
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': country_titles[country]['name'],
        # 'value': 'Share of global hashrate',
        'local_value': country_titles[country]['local_value']
    }
    rows = get_mining_provinces(country, version[1:])
    send_file_func = send_file(file_type=file_type)

    return send_file_func(
        headers,
        list(map(lambda row: {**row, 'local_value': round(row['local_value'] * 100, 2)}, rows))
    )


@bp.route('/absolute_mining_countries')
@cache_control()
def absolute_mining_countries(version=None):
    if version != 'v1.1.1' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    file_type = request.args.get('file_type', 'csv')
    headers = {
        'date': 'Date',
        'name': 'Country',
        'absolute_value': 'Estimated absolute hashrate'
    }
    rows = get_mining_countries(version[1:])
    send_file_func = send_file(file_type=file_type)

    return send_file_func(
        headers,
        rows
    )


@bp.route('/bitcoin_greenhouse_gas_emissions')
@cache_control()
def bitcoin_greenhouse_gas_emissions(version=None):
    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    price = request.args.get('price', 0.05)
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh')

    headers = {
        'date': 'Date and Time',
        'min_co2': 'Hydro-only, MtCO2e',
        'guess_co2': 'Estimated, MtCO2e',
        'max_co2': 'Coal-only, MtCO2e',
    }

    service = GreenhouseGasEmissionServiceFactory.create()

    return send_file_func(
        headers,
        service.get_flat_greenhouse_gas_emissions(float(price)),
    )


@bp.route('/total_bitcoin_greenhouse_gas_emissions')
@cache_control()
def total_bitcoin_greenhouse_gas_emissions(version=None):
    def to_dict(row):
        return {
            'date': month_name[row['date'].month] + row['date'].strftime(' %Y'),
            'v': round(row['v'], 4),
            'cumulative_v': round(row['cumulative_v'], 4),
        }

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    price = request.args.get('price', 0.05)
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh')

    headers = {
        'date': 'Month',
        'v': 'Monthly emissions, MtCO2e',
        'cumulative_v': 'Cumulative emissions, MtCO2e',
    }

    service = GreenhouseGasEmissionServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_total_greenhouse_gas_emissions(float(price))],
    )


@bp.route('/total_yearly_bitcoin_greenhouse_gas_emissions')
@cache_control()
def total_yearly_bitcoin_greenhouse_gas_emissions(version=None):
    def to_dict(row, date):
        row['date'] = date.strftime('%Y')
        return {
            'date': date.strftime('%Y'),
            'v': round(row['v'], 4),
            'cumulative_v': round(row['cumulative_v'], 4),
        }

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    price = request.args.get('price', 0.05)
    send_file_func = send_file(first_line=f'Average electricity cost assumption: {price} USD/kWh')

    headers = {
        'date': 'Year',
        'v': 'Yearly emissions, MtCO2e',
        'cumulative_v': 'Cumulative emissions, MtCO2e',
    }

    service = GreenhouseGasEmissionServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row, date) for date, row in service.get_total_yearly_greenhouse_gas_emissions(float(price))],
    )


@bp.route('/bitcoin_emission_intensity')
@cache_control()
def bitcoin_emission_intensity(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%Y-%m-%dT%H:%M:%S')
        return row

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date and Time',
        'co2_coef': 'Emission intensity, gCO2e/kWh',
    }

    service = EmissionIntensityServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_bitcoin_emission_intensity()],
    )


@bp.route('/monthly_bitcoin_power_mix')
@cache_control()
def monthly_bitcoin_power_mix(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%m/%Y')
        row['value'] = round(row['value'] * 100, 2)
        return row

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'name': 'Energy source',
        'value': "% of total",
    }

    service = PowerMixServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_monthly_data()],
    )


@bp.route('/yearly_bitcoin_power_mix')
@cache_control()
def yearly_bitcoin_power_mix(version=None):
    def to_dict(row):
        row['date'] = datetime.fromtimestamp(row['timestamp']).strftime('%m/%Y')
        return row

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'name': 'Energy source',
        'value': '% of total',
    }

    service = PowerMixServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_yearly_data()],
    )


@bp.route('/greenhouse_gas_emissions')
@cache_control()
def ghg_emissions(version=None):
    def to_dict(row):
        return {
            'country': row['name'],
            'value': round(row['value'], 2)
        }

    if version != 'v1.1.1' and version != 'v1.2.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'country': 'Country',
        'value': 'MtCO2e',
    }

    service = EmissionServiceFactory.create()

    return send_file_func(
        headers,
        [to_dict(row) for row in service.get_emissions()],
    )


@bp.route('/energy_efficiency_of_mining_hardware/daily')
@bp.route('/energy_efficiency_of_mining_hardware/daily/<price>')
@cache_control()
def energy_efficiency_of_mining_hardware_daily(version=None, price=0.05):
    if version != 'v1.4.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'lower_bound': 'Lower bound efficiency, J/Th',
        'estimated': 'Estimated efficiency, J/Th',
        'upper_bound': 'Upper bound efficiency, J/Th',
    }

    service = EnergyConsumptionServiceFactory.create()

    return send_file_func(
        headers,
        service.download_energy_efficiency_of_mining_hardware(float(price)),
    )


@bp.route('/energy_efficiency_of_mining_hardware/yearly')
@bp.route('/energy_efficiency_of_mining_hardware/yearly/<price>')
@cache_control()
def energy_efficiency_of_mining_hardware_yearly(version=None, price=0.05):
    if version != 'v1.4.0' and version != 'v1.5.0':
        raise NotImplementedError('Not Implemented')

    send_file_func = send_file()

    headers = {
        'year': 'Year',
        'lower_bound': 'Lower bound efficiency, J/Th',
        'estimated': 'Estimated efficiency, J/Th',
        'upper_bound': 'Upper bound efficiency, J/Th',
    }

    service = EnergyConsumptionServiceFactory.create()

    return send_file_func(
        headers,
        service.download_efficiency_of_mining_hardware_yearly(float(price)),
    )


@bp.route('/bitcoin_cost_of_minting/daily')
@bp.route('/bitcoin_cost_of_minting/daily/<price>')
@cache_control()
def bitcoin_cost_of_minting_daily(version=None, price=0.05):
    send_file_func = send_file()

    headers = {
        'date': 'Date',
        'lower_bound': 'Lower bound cost of minting USD',
        'estimated': 'Estimated cost of minting USD',
        'upper_bound': 'Upper bound cost of minting USD',
    }

    service = BitcoinCostOfMintingServiceFactory.create()

    return send_file_func(
        headers,
        service.download_bitcoin_cost_of_minting_chart(float(price)),
    )


@bp.route('/bitcoin_cost_of_minting/yearly')
@bp.route('/bitcoin_cost_of_minting/yearly/<price>')
@cache_control()
def bitcoin_cost_of_minting_yearly(version=None, price=0.05):

    send_file_func = send_file()

    headers = {
        'year': 'Year',
        'lower_bound': 'Lower bound cost of minting USD',
        'estimated': 'Estimated cost of minting USD',
        'upper_bound': 'Upper bound cost of minting USD',
    }

    service = BitcoinCostOfMintingServiceFactory.create()

    return send_file_func(
        headers,
        service.download_bitcoin_cost_of_minting_yearly_chart(float(price)),
    )
