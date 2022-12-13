from .eth_repository import EthRepository
from .dto.charts import \
    NetworkPowerDemandDto, \
    AnnualisedConsumptionDto, \
    TotalElectricityConsumptionDto, \
    ActiveNodeDto, \
    PowerDemandLegacyVsFutureDto, \
    ComparisonOfAnnualConsumptionDto
from .dto.download import NetworkPowerDemandDto as DownloadNetworkPowerDemandDto, \
    MonthlyTotalElectricityConsumptionDto as DownloadMonthlyTotalElectricityConsumptionDto, \
    YearlyTotalElectricityConsumptionDto as DownloadYearlyTotalElectricityConsumptionDto, \
    ClientDistributionDto as DownloadClientDistributionDto, \
    ActiveNodeDto as DownloadActiveNodeDto, \
    NodeDistributionDto as DownloadNodeDistributionDto, \
    AnnualisedConsumptionDto as DownloadAnnualisedConsumptionDto
from .dto.data import StatsDto
from helpers import send_file, is_valid_date_string_format
from  exceptions import HttpException


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
            'min_power': 'Lower Electricity Demand, kW',
            'guess_power': 'Best Electricity Demand, kW',
            'max_power': 'Upper Electricity Demand, kW',
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
                    'y': item[node],
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

    def node_distribution(self):
        chart_data = self.repository.get_node_distribution()

        return chart_data

    def download_node_distribution(self):
        chart_data = self.repository.get_node_distribution()
        send_file_func = send_file()

        return send_file_func({
            'name': 'Country',
            'number_of_nodes': 'Number of nodes',
        }, [DownloadNodeDistributionDto(x) for x in chart_data])

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
