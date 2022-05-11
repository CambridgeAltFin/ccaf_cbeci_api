
from components.energy_consumption.v1_1_1.energy_consumption_repository import EnergyConsumptionRepository as EnergyConsumptionRepository_v1_1_1

from config import Connection

from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta


class EnergyConsumptionRepository(EnergyConsumptionRepository_v1_1_1):

    def get_miners(self):
        five_years_ago = datetime.now() - relativedelta(years=5)
        sql = 'SELECT miner_name, unix_date_of_release, efficiency_j_gh, qty, type FROM miners ' \
              'WHERE is_active is true AND is_manufacturer = 1 ' \
              'AND (unix_date_of_release >= %s OR unix_date_of_release <= %s)'
        return self._run_select_query(
            sql,
            (int(five_years_ago.timestamp()), int(datetime(2014, 7, 1, tzinfo=timezone.utc).timestamp())),
            Connection.custom_data
        )
