from components.BaseRepository import CustomDataRepository


class EthRepository(CustomDataRepository):
    def get_network_power_demand(self, price: float):
        return self._run_select_query(
            "SELECT timestamp, min_power::float, guess_power::float, max_power::float "
            "FROM consumptions "
            "WHERE asset = 'eth' AND price = %s "
            "ORDER BY timestamp",
            (int(price * 100), )
        )

    def get_monthly_total_electricity_consumption(self, price: float):
        return self._run_select_query(
            "SELECT timestamp, guess_consumption::float, cumulative_guess_consumption::float "
            "FROM total_consumptions "
            "WHERE asset = 'eth' AND price = %s "
            "ORDER BY timestamp",
            (int(price * 100),)
        )

    def get_yearly_total_electricity_consumption(self, price: float):
        return self._run_select_query(
            """
            select extract(epoch from to_timestamp(t.year::text, 'YYYY'))::int as timestamp,
                   t.guess_consumption::float,
                   sum(t.guess_consumption) 
                     over (partition by t.asset order by t.year)::float as cumulative_guess_consumption
            from (SELECT asset,
                         extract(year from date) as year,
                         sum(guess_consumption)  as guess_consumption
                  from total_consumptions
                  where asset = 'eth'
                    and price = %s
                  group by asset, extract(year from date)) as t
            """,
            (int(price * 100),)
        )

    def get_machine_efficiencies(self):
        return self._run_select_query(
            'SELECT name, efficiency_gh_j * 1000 AS efficiency, extract(epoch from released_at)::int AS timestamp '
            'FROM cbsi_miners '
            'ORDER BY released_at'
        )

    def get_average_machine_efficiency(self):
        return self._run_select_query(
            "SELECT timestamp, value::float as efficiency, miners FROM average_machine_efficiencies "
            "WHERE asset = 'eth' ORDER BY timestamp"
        )

    def get_profitability_threshold(self, price):
        return self._run_select_query(
            "SELECT timestamp, machine_efficiency::float * 1000000 AS machine_efficiency "
            "FROM consumptions "
            "WHERE asset = 'eth' AND price = %s "
            "ORDER BY timestamp",
            (int(price * 100),)
        )
