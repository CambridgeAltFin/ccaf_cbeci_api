
from components.energy_consumption.v1_1_1.energy_consumption_repository import EnergyConsumptionRepository as EnergyConsumptionRepository_v1_1_1

from config import Connection


class EnergyConsumptionRepository(EnergyConsumptionRepository_v1_1_1):

    def get_miners(self):
        sql = 'SELECT miner_name ' \
              ', unix_date_of_release ' \
              ', efficiency_j_gh ' \
              ', qty ' \
              ', type ' \
              ', unix_date_of_release + 157680000 "five_years_after_release" ' \
              'FROM miners ' \
              'WHERE is_active is true AND is_manufacturer = 1 '
        return self._run_select_query(
            sql,
            (),
            Connection.custom_data
        )
