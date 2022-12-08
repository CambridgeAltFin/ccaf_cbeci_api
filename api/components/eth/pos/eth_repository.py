from components.BaseRepository import CustomDataRepository


class EthRepository(CustomDataRepository):
    def get_network_power_demand(self):
        return self._run_select_query(
            "SELECT timestamp, min_power::float, guess_power::float, max_power::float "
            "FROM consumptions "
            "WHERE asset = 'eth_pos' "
            "ORDER BY timestamp",
        )

    def get_monthly_total_electricity_consumption(self):
        return self._run_select_query(
            """
            select t.timestamp,
                   t.guess_consumption::float,
                   sum(t.guess_consumption) 
                     over (partition by t.asset order by t.month)::float as cumulative_guess_consumption
            from (SELECT asset,
                         extract(epoch from max(date))::int as timestamp,
                         substr(date::text, 1, 7) as month,
                         sum(guess_consumption)  as guess_consumption
                  from consumptions
                  where asset = 'eth_pos'
                  group by asset, substr(date::text, 1, 7)) as t
            """,
        )

    def get_yearly_total_electricity_consumption(self):
        return self._run_select_query(
            """
            select t.timestamp,
                   t.guess_consumption::float,
                   sum(t.guess_consumption) 
                     over (partition by t.asset order by t.year)::float as cumulative_guess_consumption
            from (SELECT asset,
                         extract(epoch from max(date))::int as timestamp,
                         extract(year from date) as year,
                         sum(guess_consumption)  as guess_consumption
                  from consumptions
                  where asset = 'eth_pos'
                  group by asset, extract(year from date)) as t
            """,
        )

    def get_client_distribution(self):
        return self._run_select_query(
            "SELECT prysm, lighthouse, teku, nimbus, lodestar, grandine, others, "
            "   extract(epoch from (date + interval '22 hour 30 minute'))::int as timestamp "
            "FROM eth_pos_nodes "
            "WHERE source = 'prometheus' "
            "ORDER BY date",
        )

    def get_active_nodes(self):
        return self._run_select_query(
            "SELECT prysm + lighthouse + teku + nimbus + lodestar + grandine + others as total, "
            "   extract(epoch from (date + interval '22 hour 30 minute'))::int as timestamp "
            "FROM eth_pos_nodes "
            "WHERE source = 'prometheus' "
            "ORDER BY date",
        )
