from components.BaseRepository import CustomDataRepository
from datetime import datetime, timedelta


class EthRepository(CustomDataRepository):
    def get_stats(self):
        result = self._run_select_query(
            "select"
            " min_power::float,"
            " guess_power::float,"
            " max_power::float,"
            " min_consumption / power(10, 6) as min_consumption,"  # kWh to GWh
            " guess_consumption / power(10, 6) as guess_consumption,"  # kWh to GWh
            " max_consumption / power(10, 6) as max_consumption "  # kWh to GWh
            "from consumptions "
            "where asset = 'eth_pos'"
            "order by timestamp desc "
            "limit 1"
        )
        return result[0] if len(result) else None

    def get_network_power_demand(self):
        return self._run_select_query(
            "SELECT timestamp, min_power::float, guess_power::float, max_power::float "
            "FROM consumptions "
            "WHERE asset = 'eth_pos' "
            "ORDER BY timestamp",
        )

    def get_annualised_consumption(self):
        return self._run_select_query(
            "SELECT timestamp,"
            " min_consumption / power(10, 6) as min_consumption,"
            " guess_consumption / power(10, 6) as guess_consumption,"
            " max_consumption / power(10, 6) as max_consumption "
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
                         extract(epoch from to_date(substr(date::text, 1, 7), 'yyyy-mm'))::int as timestamp,
                         substr(date::text, 1, 7) as month,
                         sum(guess_consumption) / 365.25 / power(10, 6)  as guess_consumption -- annual kWh to daily GWh
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
                         extract(epoch from to_date(extract(year from date)::varchar, 'yyyy'))::int as timestamp,
                         extract(year from date) as year,
                         sum(guess_consumption) / 365.25 / power(10, 6) as guess_consumption -- annual kWh to daily GWh
                  from consumptions
                  where asset = 'eth_pos'
                  group by asset, extract(year from date)) as t
            """,
        )

    def get_client_distribution(self):
        return self._run_select_query(
            "SELECT prysm, lighthouse, teku, nimbus, lodestar, grandine, others, "
            "   extract(epoch from (date))::int as timestamp "
            "FROM eth_pos_nodes "
            "WHERE source = 'prometheus' "
            "ORDER BY date",
        )

    def get_active_nodes(self):
        return self._run_select_query(
            "SELECT prysm + lighthouse + teku + nimbus + lodestar + grandine + others as total, "
            "   extract(epoch from (date))::int as timestamp "
            "FROM eth_pos_nodes "
            "WHERE source = 'prometheus' "
            "ORDER BY date",
        )

    def get_node_distribution(self):
        return self._run_select_query(
            "SELECT countries.country AS name, "
            "   countries.code, "
            "   countries.country_flag AS flag, "
            "   eth_pos_nodes_distribution.number_of_nodes, "
            "   eth_pos_nodes_distribution.date, "
            "   eth_pos_nodes_distribution.number_of_nodes::numeric / agg.total::numeric AS country_share "
            "FROM eth_pos_nodes_distribution "
            "JOIN countries ON eth_pos_nodes_distribution.country_id = countries.id "
            "JOIN ("
            "   SELECT date, sum(number_of_nodes) AS total "
            "   FROM eth_pos_nodes_distribution "
            "   WHERE eth_pos_nodes_distribution.source = 'prometheus'"
            "   GROUP BY date"
            ") AS agg ON agg.date = eth_pos_nodes_distribution.date "
            "WHERE eth_pos_nodes_distribution.source = 'prometheus' "
            "ORDER BY countries.country, eth_pos_nodes_distribution.date"
        )

    def get_node_distribution_by_date(self, date):
        return self._run_select_query(
            "SELECT countries.country AS name, "
            "   countries.code, "
            "   countries.country_flag AS flag, "
            "   eth_pos_nodes_distribution.number_of_nodes, "
            "   eth_pos_nodes_distribution.date, "
            "   eth_pos_nodes_distribution.number_of_nodes::numeric / agg.total::numeric AS country_share "
            "FROM eth_pos_nodes_distribution "
            "JOIN countries ON eth_pos_nodes_distribution.country_id = countries.id "
            "JOIN ("
            "   SELECT date, sum(number_of_nodes) AS total "
            "   FROM eth_pos_nodes_distribution "
            "   WHERE eth_pos_nodes_distribution.source = 'prometheus' AND date(eth_pos_nodes_distribution.date) = %s"
            "   GROUP BY date"
            ") AS agg ON agg.date = eth_pos_nodes_distribution.date "
            "WHERE eth_pos_nodes_distribution.source = 'prometheus' "
            "ORDER BY countries.country, eth_pos_nodes_distribution.date",
            ((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d'),)
        )

    def get_power_demand_legacy_vs_future(self):
        return self._run_select_query("""
            select distinct on (asset) asset,
                               case
                                   when asset = 'eth' then guess_power * power(10, 3) -- GW to MW
                                   else guess_power / power(10, 3) -- kW to MW
                               end as guess_power
            from consumptions
            where asset = 'eth_pos' or (asset = 'eth' and price = 5)
            order by asset, timestamp desc
        """)

    def get_power_demand_legacy_vs_future_by_date(self, date):
        return self._run_select_query("""
            select distinct on (asset) asset,
                               case
                                   when asset = 'eth' then guess_power * power(10, 3) -- GW to MW
                                   else guess_power / power(10, 3) -- kW to MW
                               end as guess_power
            from consumptions
            where (asset = 'eth_pos' or (asset = 'eth' and price = 5)) and date <= %s
            order by asset, timestamp desc
        """, (date,))

    def get_comparison_of_annual_consumption(self):
        return self._run_select_query("""
            select distinct on (asset) asset,
                               case
                                   when asset = 'eth_pos' then guess_consumption / power(10, 9) -- kWh to TWh
                                   else guess_consumption end as guess_consumption
            from consumptions
            where asset = 'eth_pos'
               or (asset in ('btc', 'eth') and price = 5)
            order by asset, timestamp desc
        """)

    def get_comparison_of_annual_consumption_by_date(self, date):
        return self._run_select_query("""
            select distinct on (asset) asset,
                               case
                                   when asset = 'eth_pos' then guess_consumption / power(10, 9) -- kWh to TWh
                                   else guess_consumption end as guess_consumption
            from consumptions
            where (asset = 'eth_pos' or (asset in ('btc', 'eth') and price = 5))
               and date <= %s
            order by asset, timestamp desc
        """, (date,))
