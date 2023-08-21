
from components.energy_consumption.v1_3_1.energy_consumption_repository import EnergyConsumptionRepository as EnergyConsumptionRepository_v1_3_1

from config import Connection


class EnergyConsumptionRepository(EnergyConsumptionRepository_v1_3_1):
    def get_miners(self):
        sql = '''
            SELECT miner_name
             , unix_date_of_release
             , extract(epoch from to_timestamp(unix_date_of_release) + interval '2 months') as "date_of_release"
             , efficiency_j_gh
             , power
             , hashing_power
             , qty
             , type
             , unix_date_of_release + 157680000 as "five_years_after_release"
            FROM miners
            WHERE is_active is true
              AND is_manufacturer = 1
        '''
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )

    def get_daily_profitability_equipment(self):
        sql = '''
            SELECT date,
             lower_bound * 1000 as lower_bound, 
             estimated * 1000 as estimated, 
             upper_bound  * 1000 as upper_bound
            FROM miner_energy_efficients 
            ORDER BY date
        '''
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )

    def get_yearly_profitability_equipment(self):
        sql = '''
            SELECT extract(year from date) * 1000 as date,
                   avg(lower_bound) * 1000        as lower_bound,
                   avg(estimated) * 1000          as estimated,
                   avg(upper_bound) * 1000        as upper_bound
            FROM miner_energy_efficients
            GROUP BY extract(year from date)
            ORDER BY date
        '''
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )
