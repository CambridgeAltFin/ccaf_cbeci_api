
from components.energy_consumption.v1_3_1.energy_consumption_repository import EnergyConsumptionRepository as EnergyConsumptionRepository_v1_3_1

from config import Connection


class BitcoinCostOfMintingRepository(EnergyConsumptionRepository_v1_3_1):

    def get_rev_has_rate_ntv_day(self):
        sql = '''
            SELECT date,
             rev_has_rate_ntv_day
            FROM rev_has_rate_ntv
            ORDER BY date
        '''
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )

    def get_rev_has_rate_ntv_day_yearly(self):
        sql = '''
            SELECT SUBSTRING(date,0,5) as date,
                   avg(rev_has_rate_ntv_day) as rev_has_rate_ntv_day
            FROM rev_has_rate_ntv
            GROUP BY SUBSTRING(date,0,5)
            ORDER BY date
        '''
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )
