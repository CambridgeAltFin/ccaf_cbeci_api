from calendar import month_name

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
            'min_consumption': 'annualised consumption MIN, TWh',
            'guess_consumption': 'annualised consumption GUESS, TWh',
            'max_consumption': 'annualised consumption MAX, TWh',
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

    def source_comparison(self):
        chart_data = self.repository.get_source_comparison()

        return [{
            'name': i['label'],
            'x': i['timestamp'],
            'y': round(i['value'], 4),
        } for i in chart_data]

    def download_source_comparison(self):
        chart_data = self.repository.get_source_comparison_for_download()
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date',
            'cbnsi': 'CBNSI, TWh',
            'ccri': 'CCRI, TWh',
            'km': 'Kyle McDonald, TWh',
            'digiconomist': 'Digiconomist, TWh',
        }, [{
            'date': i['date'].strftime('%Y-%m-%d'),
            'cbnsi': round(i['cbnsi'], 4) if i['cbnsi'] else '',
            'ccri': round(i['ccri'], 4) if i['ccri'] else '',
            'km': round(i['km'], 4) if i['km'] else '',
            'digiconomist': round(i['digiconomist'], 4) if i['digiconomist'] else '',
        } for i in chart_data])

    def greenhouse_gas_emissions(self, price):
        chart_data = self.repository.get_greenhouse_gas_emissions(price)

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'name': x['name'],
            }
            for x in chart_data
        ]

    def download_greenhouse_gas_emissions(self, price):
        chart_data = self.repository.get_flat_greenhouse_gas_emissions(price)
        send_file_func = send_file()

        return send_file_func({
            'date': 'Date and Time',
            'min_co2': 'Hydro-only, MtCO2e',
            'guess_co2': 'Estimated, MtCO2e',
            'max_co2': 'Coal-only, MtCO2e',
        }, chart_data)

    def total_greenhouse_gas_emissions_monthly(self, price):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_monthly(price)

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'cumulative_y': round(x['cumulative_value'], 2),
            }
            for x in chart_data
        ]

    def download_total_greenhouse_gas_emissions_monthly(self, price):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_monthly(price)
        send_file_func = send_file()

        return send_file_func({
            'date': 'Month',
            'value': 'Monthly emissions, MtCO2e',
            'cumulative_value': 'Cumulative emissions, MtCO2e',
        }, [
            {
                'date': month_name[x['date'].month] + x['date'].strftime(' %Y'),
                'value': round(x['value'], 4),
                'cumulative_value': round(x['cumulative_value'], 4),
            }
            for x in chart_data
        ])

    def total_greenhouse_gas_emissions_yearly(self, price):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_yearly(price)

        return [
            {
                'x': x['timestamp'],
                'y': round(x['value'], 2),
                'cumulative_y': round(x['cumulative_value'], 2),
            }
            for x in chart_data
        ]

    def download_total_greenhouse_gas_emissions_yearly(self, price):
        chart_data = self.repository.get_total_greenhouse_gas_emissions_yearly(price)
        send_file_func = send_file()

        return send_file_func({
            'date': 'Year',
            'value': 'Yearly emissions, MtCO2e',
            'cumulative_value': 'Cumulative emissions, MtCO2e',
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
