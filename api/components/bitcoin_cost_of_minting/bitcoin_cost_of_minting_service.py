from components.bitcoin_cost_of_minting.bitcoin_cost_of_minting_repository import BitcoinCostOfMintingRepository
from datetime import datetime
import pandas as pd
from components.energy_consumption import EnergyConsumptionServiceFactory

class BitcoinCostOfMintingService:
    # that is because base calculation in the DB is for the price 0.05 USD/KWth
    default_price = 0.05

    def __init__(
        self,
        repository: BitcoinCostOfMintingRepository
    ):
        self.repository = repository


    def energy_efficiency_of_mining_hardware_chart(self, price: float):
        service = EnergyConsumptionServiceFactory.create()
        equipment_list = service.get_equipment_list(price)

        return [{
            'date': item['date'],
            'lower_bound': round(item['profitability_equipment_lower_bound'] * 10000 * 1000) / 10000,
            'estimated': round(item['profitability_equipment'] * 10000 * 1000) / 10000,
            'upper_bound': round(item['profitability_equipment_upper_bound'] * 10000 * 1000) / 10000,
        } for item in equipment_list]

    def bitcoin_cost_of_minting_chart(self, price: float):
        energy_efficiency = pd.DataFrame.from_records(self.energy_efficiency_of_mining_hardware_chart(float(price)))

        rev_has_rate_ntv = pd.DataFrame(self.repository.get_rev_has_rate_ntv_day())

        marged = pd.merge(rev_has_rate_ntv, energy_efficiency, left_on='date', right_on='date', how="inner")

        marged['price_min'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['lower_bound'])
        marged['price_estimated'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['estimated'])
        marged['price_max'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['upper_bound'])

        return [{
            'x': int(datetime.strptime(item['date'], "%Y-%m-%d").timestamp()),
            'lower_bound': item['price_min'],
            'estimated': item['price_estimated'],
            'upper_bound': item['price_max'],
        } for index, item in marged.iterrows()]

    def download_bitcoin_cost_of_minting_chart(self, price: float):
        energy_efficiency = pd.DataFrame.from_records(self.energy_efficiency_of_mining_hardware_chart(float(price)))

        rev_has_rate_ntv = pd.DataFrame(self.repository.get_rev_has_rate_ntv_day())

        marged = pd.merge(rev_has_rate_ntv, energy_efficiency, left_on='date', right_on='date', how="inner")

        marged['price_min'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['lower_bound'])
        marged['price_estimated'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['estimated'])
        marged['price_max'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['upper_bound'])
        return [{
            'date': item['date'],
            'lower_bound': item['price_min'],
            'estimated': item['price_estimated'],
            'upper_bound': item['price_max'],
        } for index, item in marged.iterrows()]

    def bitcoin_cost_of_minting_yearly_chart(self, price: float):
        energy_efficiency = pd.DataFrame.from_records(self.energy_efficiency_of_mining_hardware_chart(float(price)))

        energy_efficiency['date'] = pd.to_datetime(energy_efficiency['date'])
        df = energy_efficiency.groupby(energy_efficiency['date'].dt.year).agg(
            {
                'lower_bound': 'mean',
                'estimated': 'mean',
                'upper_bound': 'mean',
             }
        )

        df = df.reset_index()
        rev_has_rate_ntv = pd.DataFrame(self.repository.get_rev_has_rate_ntv_day_yearly())
        df['date'] = df['date'].astype("string")

        rev_has_rate_ntv['date'] = rev_has_rate_ntv['date'].astype("string")

        marged = pd.merge(rev_has_rate_ntv, df, left_on='date', right_on='date', how="inner")

        marged['price_min'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['lower_bound'])
        marged['price_estimated'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['estimated'])
        marged['price_max'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['upper_bound'])
        return [{
            'x': int(datetime.strptime(str(int(item['date'])), '%Y').timestamp()),
            'lower_bound': item['price_min'],
            'estimated': item['price_estimated'],
            'upper_bound': item['price_max'],
        } for index, item in marged.iterrows()]

    def download_bitcoin_cost_of_minting_yearly_chart(self, price: float):
        energy_efficiency = pd.DataFrame.from_records(self.energy_efficiency_of_mining_hardware_chart(float(price)))

        energy_efficiency['date'] = pd.to_datetime(energy_efficiency['date'])
        df = energy_efficiency.groupby(energy_efficiency['date'].dt.year).agg(
            {
                'lower_bound': 'mean',
                'estimated': 'mean',
                'upper_bound': 'mean',
            }
        )

        df = df.reset_index()
        rev_has_rate_ntv = pd.DataFrame(self.repository.get_rev_has_rate_ntv_day_yearly())
        df['date'] = df['date'].astype("string")

        rev_has_rate_ntv['date'] = rev_has_rate_ntv['date'].astype("string")

        marged = pd.merge(rev_has_rate_ntv, df, left_on='date', right_on='date', how="inner")

        marged['price_min'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['lower_bound'])
        marged['price_estimated'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['estimated'])
        marged['price_max'] = price / (3600000 * marged['rev_has_rate_ntv_day'] / marged['upper_bound'])
        return [{
            'year': item['date'],
            'lower_bound': item['price_min'],
            'estimated': item['price_estimated'],
            'upper_bound': item['price_max'],
        } for index, item in marged.iterrows()]
