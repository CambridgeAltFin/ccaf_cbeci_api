from .eth_repository import EthRepository
from .dto.charts import \
    NetworkPowerDemandDto, \
    AnnualisedConsumptionDto, \
    TotalElectricityConsumptionDto, \
    MachineEfficiencyDto, \
    AverageMachineEfficiencyDto, \
    MiningEquipmentEfficiencyDto, \
    ProfitabilityThresholdDto
from .dto.download import NetworkPowerDemandDto as DownloadNetworkPowerDemandDto, \
    MonthlyTotalElectricityConsumptionDto as DownloadMonthlyTotalElectricityConsumptionDto, \
    AnnualisedConsumptionDto as DownloadAnnualisedConsumptionDto, \
    YearlyTotalElectricityConsumptionDto as DownloadYearlyTotalElectricityConsumptionDto, \
    MiningEquipmentEfficiencyDto as DownloadMiningEquipmentEfficiencyDto, \
    NetworkEfficiencyDto as DownloadNetworkEfficiencyDto, \
    ProfitabilityThresholdDto as DownloadProfitabilityThresholdDto
from .dto.data import StatsDto
from helpers import send_file


class EthService:
    def __init__(self, repository: EthRepository):
        self.repository = repository

    def stats(self, price: float):
        stats = self.repository.get_stats(price)
        return StatsDto(stats)

    def network_power_demand(self, price: float) -> list[NetworkPowerDemandDto]:
        chart_data = self.repository.get_network_power_demand(price)

        return list(map(lambda x: NetworkPowerDemandDto(x), chart_data))

    def download_network_power_demand(self, price: float):
        chart_data = self.repository.get_network_power_demand(price)
        send_file_func = send_file(first_line=f'Average electricity cost assumption: {str(price)} USD/kWh')

        return send_file_func({
            'timestamp': 'Date and Time',
            'min_power': 'power MIN, GW',
            'guess_power': 'power GUESS, GW',
            'max_power': 'power MAX, GW',
        }, list(map(lambda x: DownloadNetworkPowerDemandDto(x), chart_data)))

    def annualised_consumption(self, price: float) -> list[AnnualisedConsumptionDto]:
        chart_data = self.repository.get_annualised_consumption(price)

        return [AnnualisedConsumptionDto(x) for x in chart_data]

    def download_annualised_consumption(self, price: float):
        chart_data = self.repository.get_annualised_consumption(price)
        send_file_func = send_file(first_line=f'Average electricity cost assumption: {str(price)} USD/kWh')

        return send_file_func({
            'timestamp': 'Date and Time',
            'min_consumption': 'annualised consumption MIN, TWh',
            'guess_consumption': 'annualised consumption GUESS, TWh',
            'max_consumption': 'annualised consumption MAX, TWh',
        }, [DownloadAnnualisedConsumptionDto(x) for x in chart_data])

    def monthly_total_electricity_consumption(self, price: float) -> list[TotalElectricityConsumptionDto]:
        chart_data = self.repository.get_monthly_total_electricity_consumption(price)

        return list(map(lambda x: TotalElectricityConsumptionDto(x), chart_data))

    def yearly_total_electricity_consumption(self, price: float) -> list[TotalElectricityConsumptionDto]:
        chart_data = self.repository.get_yearly_total_electricity_consumption(price)

        return list(map(lambda x: TotalElectricityConsumptionDto(x), chart_data))

    def download_monthly_total_electricity_consumption(self, price: float):
        chart_data = self.repository.get_monthly_total_electricity_consumption(price)
        send_file_func = send_file(first_line=f'Average electricity cost assumption: {str(price)} USD/kWh')

        return send_file_func({
            'timestamp': 'Month',
            'consumption': 'Monthly consumption, TWh',
            'cumulative_consumption': 'Cumulative consumption, TWh',
        }, list(map(lambda x: DownloadMonthlyTotalElectricityConsumptionDto(x), chart_data)))

    def download_yearly_total_electricity_consumption(self, price: float):
        chart_data = self.repository.get_yearly_total_electricity_consumption(price)
        send_file_func = send_file(first_line=f'Average electricity cost assumption: {str(price)} USD/kWh')

        return send_file_func({
            'timestamp': 'Year',
            'consumption': 'Yearly consumption, TWh',
            'cumulative_consumption': 'Cumulative consumption, TWh',
        }, list(map(lambda x: DownloadYearlyTotalElectricityConsumptionDto(x), chart_data)))

    def network_efficiency(self):
        machine_efficiencies = self.repository.get_machine_efficiencies()
        average_machine_efficiency = self.repository.get_average_machine_efficiency()

        return {
            'machine_efficiencies': list(map(lambda x: MachineEfficiencyDto(x), machine_efficiencies)),
            'average_machine_efficiency': list(map(
                lambda x: AverageMachineEfficiencyDto(x),
                average_machine_efficiency
            )),
        }

    def download_network_efficiency(self):
        chart_data = self.repository.get_average_machine_efficiency()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'miners': 'Equipment efficiency',
            'efficiency': 'Average equipment efficiency, Mh/J',
        }, list(map(lambda x: DownloadNetworkEfficiencyDto(x), chart_data)))

    def mining_equipment_efficiency(self) -> list[MiningEquipmentEfficiencyDto]:
        chart_data = self.repository.get_machine_efficiencies()

        return list(map(lambda x: MiningEquipmentEfficiencyDto(x), chart_data))

    def download_mining_equipment_efficiency(self):
        chart_data = self.repository.get_machine_efficiencies()
        send_file_func = send_file()

        return send_file_func({
            'timestamp': 'Date and Time',
            'name': 'Miner',
            'efficiency': 'Efficiency, Mh/J',
        }, list(map(lambda x: DownloadMiningEquipmentEfficiencyDto(x), chart_data)))

    def profitability_threshold(self, price: float):
        chart_data = self.repository.get_profitability_threshold(price)

        return list(map(lambda x: ProfitabilityThresholdDto(x), chart_data))

    def download_profitability_threshold(self, price: float):
        chart_data = self.repository.get_profitability_threshold(price)
        send_file_func = send_file(first_line=f'Average electricity cost assumption: {str(price)} USD/kWh')

        return send_file_func({
            'timestamp': 'Date and Time',
            'efficiency': 'Efficiency, Mh/J',
        }, list(map(lambda x: DownloadProfitabilityThresholdDto(x), chart_data)))
