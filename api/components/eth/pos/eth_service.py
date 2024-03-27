import calendar
from .eth_repository import EthRepository
from .dto.charts import \
    NetworkPowerDemandDto, \
    AnnualisedConsumptionDto, \
    TotalElectricityConsumptionDto, \
    ActiveNodeDto, \
    PowerDemandLegacyVsFutureDto, \
    NodeDistributionDto, \
    MonthlyNodeDistributionDto, \
    NodeDistributionMetaDto, \
    MonthlyNodeDistributionMetaDto, \
    ComparisonOfAnnualConsumptionDto
from .dto.download import NetworkPowerDemandDto as DownloadNetworkPowerDemandDto, \
    MonthlyTotalElectricityConsumptionDto as DownloadMonthlyTotalElectricityConsumptionDto, \
    YearlyTotalElectricityConsumptionDto as DownloadYearlyTotalElectricityConsumptionDto, \
    ClientDistributionDto as DownloadClientDistributionDto, \
    ActiveNodeDto as DownloadActiveNodeDto, \
    NodeDistributionDto as DownloadNodeDistributionDto, \
    MonthlyNodeDistributionDto as DownloadMonthlyNodeDistributionDto, \
    AnnualisedConsumptionDto as DownloadAnnualisedConsumptionDto
from .dto.data import StatsDto
from helpers import send_file, is_valid_date_string_format
from exceptions import HttpException
import datetime
from calendar import month_name
from dateutil.relativedelta import relativedelta

class EthService:
    def __init__(self, repository: EthRepository):
        self.repository = repository

    def stats(self):
        stats = self.repository.get_stats()
        return StatsDto(stats)

    def network_power_demand(self) -> list[NetworkPowerDemandDto]:
        chart_data = self.repository.get_network_power_demand()

        return list(map(lambda x: NetworkPowerDemandDto(x), chart_data))

    def download_network_power_demand(self):
        chart_data = self.repository.get_network_power_demand()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'min_power': 'power MIN, kW',
            'guess_power': 'power GUESS, kW',
            'max_power': 'power MAX, kW',
            'min_consumption': 'annualised consumption MIN, GWh',
            'guess_consumption': 'annualised consumption GUESS, GWh',
            'max_consumption': 'annualised consumption MAX, GWh',
        }, list(map(lambda x: DownloadNetworkPowerDemandDto(x), chart_data)))

    def annualised_consumption(self) -> list[AnnualisedConsumptionDto]:
        chart_data = self.repository.get_annualised_consumption()

        return [AnnualisedConsumptionDto(x) for x in chart_data]

    def download_annualised_consumption(self):
        chart_data = self.repository.get_annualised_consumption()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'min_consumption': 'Lower Annualised Consumption, GWh',
            'guess_consumption': 'Best Annualised Consumption, GWh',
            'max_consumption': 'Upper Annualised Consumption, GWh',
        }, [DownloadAnnualisedConsumptionDto(x) for x in chart_data])

    def monthly_total_electricity_consumption(self) -> list[TotalElectricityConsumptionDto]:
        chart_data = self.repository.get_monthly_total_electricity_consumption()

        return list(map(lambda x: TotalElectricityConsumptionDto(x), chart_data))

    def yearly_total_electricity_consumption(self) -> list[TotalElectricityConsumptionDto]:
        chart_data = self.repository.get_yearly_total_electricity_consumption()

        return list(map(lambda x: TotalElectricityConsumptionDto(x), chart_data))

    def download_monthly_total_electricity_consumption(self):
        chart_data = self.repository.get_monthly_total_electricity_consumption()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Month',
            'consumption': 'Monthly consumption, GWh',
            'cumulative_consumption': 'Cumulative consumption, GWh',
        }, list(map(lambda x: DownloadMonthlyTotalElectricityConsumptionDto(x), chart_data)))

    def download_yearly_total_electricity_consumption(self):
        chart_data = self.repository.get_yearly_total_electricity_consumption()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Year',
            'consumption': 'Yearly consumption, GWh',
            'cumulative_consumption': 'Cumulative consumption, GWh',
        }, list(map(lambda x: DownloadYearlyTotalElectricityConsumptionDto(x), chart_data)))

    def client_distribution(self):
        client_distribution = self.repository.get_client_distribution()
        chart_data = []
        for item in client_distribution:
            timestamp = item.pop('timestamp')
            for node in item:
                chart_data.append({
                    'name': str(node).capitalize(),
                    'x': timestamp,
                    'y': float(round(item[node], 4)),
                })
        return chart_data

    def download_client_distribution(self):
        chart_data = self.repository.get_client_distribution()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'prysm': 'Prysm',
            'lighthouse': 'Lighthouse',
            'teku': 'Teku',
            'nimbus': 'Nimbus',
            'lodestar': 'Lodestar',
            'grandine': 'Grandine',
            'erigon': 'Erigon',
            'others': 'Others',
        }, [DownloadClientDistributionDto(x) for x in chart_data])

    def active_nodes(self) -> list[ActiveNodeDto]:
        chart_data = self.repository.get_active_nodes()

        return [ActiveNodeDto(x) for x in chart_data]

    def download_active_nodes(self):
        chart_data = self.repository.get_active_nodes()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'total': 'Number of nodes',
        }, [DownloadActiveNodeDto(x) for x in chart_data])

    def node_distribution(self, date: str = None):
        if date is None or not is_valid_date_string_format(date):
            date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        chart_data = self.repository.get_node_distribution_by_date(date)
        meta = self.repository.get_node_distribution_meta()

        return [NodeDistributionDto(x) for x in chart_data], NodeDistributionMetaDto(meta)

    def download_node_distribution(self):
        chart_data = self.repository.get_node_distribution()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date and Time',
            'name': 'Country',
            'country_share': "Country's share, %"
        }, [DownloadNodeDistributionDto(x) for x in chart_data])

    def monthly_node_distribution(self, date: str = None):
        if date is None or not is_valid_date_string_format(date, '%Y-%m'):
            date = datetime.date.today().strftime('%Y-%m')
        chart_data = self.repository.get_monthly_node_distribution_by_date(date)
        meta = self.repository.get_monthly_node_distribution_meta()

        return [MonthlyNodeDistributionDto(x) for x in chart_data], MonthlyNodeDistributionMetaDto(meta)

    def download_monthly_node_distribution(self):
        chart_data = self.repository.get_monthly_node_distribution()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Month',
            'name': 'Country',
            'country_share': "Country's share, %"
        }, [DownloadMonthlyNodeDistributionDto(x) for x in chart_data])

    def power_demand_legacy_vs_future(self, date: str = None) -> list[PowerDemandLegacyVsFutureDto]:
        if date is None:
            chart_data = self.repository.get_power_demand_legacy_vs_future()
        elif is_valid_date_string_format(date):
            chart_data = self.repository.get_power_demand_legacy_vs_future_by_date(date)
        else:
            raise HttpException(f'Invalid date: {date}')

        return [PowerDemandLegacyVsFutureDto(x) for x in chart_data]

    def comparison_of_annual_consumption(self, date: str = None) -> list[ComparisonOfAnnualConsumptionDto]:
        if date is None:
            chart_data = self.repository.get_comparison_of_annual_consumption()
        elif is_valid_date_string_format(date):
            chart_data = self.repository.get_comparison_of_annual_consumption_by_date(date)
        else:
            raise HttpException(f'Invalid date: {date}')

        return [ComparisonOfAnnualConsumptionDto(x) for x in chart_data]

    def get_live_data(self):
        stats = self.repository.get_stats()
        live = self.repository.get_live_data()

        return {
            'cbnsi': str(round(stats['guess_consumption'], 2)) + ' GWh',
            'ccri': str(live[0]['value']) + ' GWh',
            'digiconomist': str(live[1]['value']) + ' GWh',
        }
    
    def total_number_of_active_validators(self):
        chart_data = self.repository.active_validators_total()

        return [
            {
                'x': x['timestamp'],
                'y': x['total']
            }
            for x in chart_data
        ]
    
    def download_total_number_of_active_validators(self):
        chart_data = self.repository.active_validators_total()
        send_file_func = send_file()

        return send_file_func({
            'x': 'timestamp',
            'y': 'total',
        }, [
            {
                'x': x['timestamp'],
                'y': x['total']
            }
            for x in chart_data
        ])
    
    def market_share_of_staking_entities(self, date: str = None):
        end_date = None
        if not (date is None) and is_valid_date_string_format(date):
            date_temp = datetime.datetime.strptime(date, '%Y-%m-%d').date()
            date = calendar.timegm(datetime.date(date_temp.year, date_temp.month, 1).timetuple())
            end_date = calendar.timegm((datetime.date(date_temp.year, date_temp.month, 1) + relativedelta(months=1)).timetuple())
        elif not (date is None):
            raise HttpException(f'Invalid date: {date}')

        chart_data = self.repository.active_validators(date, end_date)

        result = []
        prev_date = None
        total = 0
        month_data = []
        for i in range(len(chart_data)):
            cur_date = datetime.date.fromtimestamp(chart_data[i]["timestamp"])
            if (not prev_date):
                prev_date = cur_date

            if (cur_date.month != prev_date.month or cur_date.year != prev_date.year):
                for j in range(len(month_data)):
                    month_data[j]['share'] = round(month_data[j]['total'] / total * 100, 2)
                result.append({
                    'timestamp': calendar.timegm(datetime.date(prev_date.year, prev_date.month, 1).timetuple()),
                    'data': month_data,
                    'total': total
                })

                total = 0
                month_data = []

            value = chart_data[i]["value"]
            entity = chart_data[i]["entity"]

            found = False
            for j in range(len(month_data)):
                if (month_data[j]['name'] == entity or entity.find('_lido') != -1 and month_data[j]['name'] == 'lido'):
                    found = True
                    month_data[j]['total'] += value
                    break

            if (not found):
                if (entity.find('_lido') != -1):
                    month_data.append({
                        'name': 'lido',
                        'total': value,
                        'share': 0,
                    })
                else:
                    month_data.append({
                        'name': entity,
                        'total': value,
                        'share': 0,
                    })

            total += value
            prev_date = cur_date

        if (prev_date):
            for j in range(len(month_data)):
                month_data[j]['share'] = round(month_data[j]['total'] / total * 100, 2)
            result.append({
                'timestamp': calendar.timegm(datetime.date(prev_date.year, prev_date.month, 1).timetuple()),
                'data': month_data,
                'total': total
            })

        return result
    
    def download_market_share_of_staking_entities(self):
        chart_data = self.repository.active_validators()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'timestamp',
            'entity': 'entity',
            'value': 'active_validators',
        }, [
            {
                'timestamp': x['timestamp'],
                'entity': x['entity'],
                'value': x['value']
            }
            for x in chart_data
        ])
    
    def staking_entities_categorization(self, date: str = None):
        if not (date is None) and is_valid_date_string_format(date):
            date = calendar.timegm(datetime.datetime.strptime(date, '%Y-%m-%d').date().timetuple())
        elif not (date is None):
            raise HttpException(f'Invalid date: {date}')

        chart_data = self.repository.staking_entities_categorization(date)

        result = []
        day_data = []
        total = 0
        prev_timestamp = None
        if(len(chart_data) > 0):
            prev_timestamp = chart_data[0]["timestamp"]
        
        for row in chart_data:
            if (row["timestamp"] != prev_timestamp):
                for j in range(len(day_data)):
                    day_data[j]['share'] = round(day_data[j]['value'] / total * 100, 2)
                result.append({
                    'timestamp': calendar.timegm(datetime.date.fromtimestamp(prev_timestamp).timetuple()),
                    'data': day_data,
                    'total': total
                })

                total = 0
                day_data = []

            day_data.append({
                'category': row['category'],
                'value': row['node_count'],
                'share': 0,
            })

            total += row['node_count']
            prev_timestamp = row["timestamp"]

        if (prev_timestamp):
            for j in range(len(day_data)):
                day_data[j]['share'] = round(day_data[j]['value'] / total * 100, 2)
            result.append({
                'timestamp': calendar.timegm(datetime.date.fromtimestamp(prev_timestamp).timetuple()),
                'data': day_data,
                'total': total
            })

        return result
    
    def download_staking_entities_categorization(self):
        chart_data = self.repository.staking_entities_categorization()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'timestamp',
            'category': 'hosting_type',
            'node_count': 'node_count',
        }, [
            {
                'timestamp': x['timestamp'],
                'category': x['category'],
                'node_count': x['node_count']
            }
            for x in chart_data
        ])
    
    def hosting_providers(self, date: str):
        if is_valid_date_string_format(date):
            date = calendar.timegm(datetime.datetime.strptime(date, '%Y-%m-%d').date().timetuple())
        else:
            raise HttpException(f'Invalid date: {date}')
        
        chart_data = self.repository.hosting_providers(date)

        top_count = 9
        data = []
        total = sum(x['node_count'] for x in chart_data)
        for row in chart_data:
            if(top_count <= 0):
                break

            if(row['isp'] != 'Other'):
                data.append({
                    'isp': row['isp'],
                    'value': row['node_count'],
                    'share': round(row['node_count'] / total * 100, 2)
                })
                chart_data.remove(row)
                top_count -= 1

        if(len(chart_data)):
            others_total = sum(x['node_count'] for x in chart_data)
            data.append({
                'isp': 'Other',
                'value': others_total,
                'share': round(others_total / total * 100, 2)
            })

        return data
    
    def download_hosting_providers(self):
        chart_data = self.repository.hosting_providers()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'timestamp',
            'isp': 'isp',
            'node_count': 'node_count',
        }, [
            {
                'timestamp': x['timestamp'],
                'isp': x['isp'],
                'node_count': x['node_count']
            }
            for x in chart_data
        ])

    def greenhouse_gas_emissions(self):
        chart_data = self.repository.get_greenhouse_gas_emissions()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_greenhouse_gas_emissions(self):
        chart_data = self.repository.get_flat_greenhouse_gas_emissions()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date and Time',
            'min_co2': 'Hydro-only, KtCO2e',
            'guess_co2': 'Estimated, KtCO2e',
            'max_co2': 'Coal-only, KtCO2e',
        }, chart_data)

    def total_greenhouse_gas_emissions_monthly(self):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_monthly()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'cumulative_y': round(x['cumulative_value'], 2),
            }
            for x in chart_data
        ]

    def download_total_greenhouse_gas_emissions_monthly(self):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_monthly()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Month',
            'value': 'Monthly emissions, KtCO2e',
            'cumulative_value': 'Cumulative emissions, KtCO2e',
        }, [
            {
                'date': month_name[x['date'].month] + x['date'].strftime(' %Y'),
                'value': round(x['value'], 4),
                'cumulative_value': round(x['cumulative_value'], 4),
            }
            for x in chart_data
        ])

    def total_greenhouse_gas_emissions_yearly(self):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_yearly()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'cumulative_y': round(x['cumulative_value'], 2),
            }
            for x in chart_data
        ]

    def download_total_greenhouse_gas_emissions_yearly(self):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_yearly()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Year',
            'value': 'Yearly emissions, KtCO2e',
            'cumulative_value': 'Cumulative emissions, KtCO2e',
        }, [
            {
                'date': x['date'],
                'value': round(x['value'], 4),
                'cumulative_value': round(x['cumulative_value'], 4),
            }
            for x in chart_data
        ])

    def monthly_power_mix(self):
        chart_data = self.repository.get_monthly_power_mix()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 4),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_monthly_power_mix(self):
        chart_data = self.repository.get_monthly_power_mix()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date',
            'name': 'Energy source',
            'value': "% of total",
        }, [
            {
                'date': month_name[x['date'].month] + x['date'].strftime(' %Y'),
                'value': round(x['value'], 4),
                'name': x['name'],
            }
            for x in chart_data
        ])

    def yearly_power_mix(self):
        chart_data = self.repository.get_yearly_power_mix()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 4),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_yearly_power_mix(self):
        chart_data = self.repository.get_yearly_power_mix()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date',
            'name': 'Energy source',
            'value': "% of total",
        }, [
            {
                'date': x['date'].strftime('%Y'),
                'value': round(x['value'], 4),
                'name': x['name'],
            }
            for x in chart_data
        ])

    def emission_intensity(self):
        chart_data = self.repository.get_emission_intensity()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_emission_intensity(self):
        chart_data = self.repository.get_emission_intensity()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date and Time',
            'value': 'Emission intensity, gCO2e/kWh',
        }, [
            {
                'date': x['date'].strftime('%Y-%m-%d %H:%M:%S'),
                'value': round(x['value'], 4),
            }
            for x in chart_data
        ])

    def monthly_emission_intensity(self):
        chart_data = self.repository.get_monthly_emission_intensity()

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_monthly_emission_intensity(self):
        chart_data = self.repository.get_monthly_emission_intensity()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date and Time',
            'value': 'Emission intensity, gCO2e/kWh',
        }, [
            {
                'date': x['date'].strftime('%Y-%m-%d %H:%M:%S'),
                'value': round(x['value'], 4),
            }
            for x in chart_data
        ])

    def get_ghg_live_data(self):
        digiconomist = self.repository.digiconomist_live_data()
        carbon_ratings = self.repository.carbon_ratings_live_data()
        cbnsi = self.repository.ghg_live_data()

        return {
            'cbnsi': str(round(cbnsi['value'], 2)),
            'carbon_ratings': str(round(carbon_ratings['value'], 2)),
            'digiconomist': str(round(digiconomist['value'], 2)),
        }
