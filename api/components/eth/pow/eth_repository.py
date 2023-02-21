from components.BaseRepository import CustomDataRepository


class EthRepository(CustomDataRepository):
    def get_stats(self, price: float):
        result = self._run_select_query(
            "select"
            " min_power::float,"
            " guess_power::float,"
            " max_power::float,"
            " min_consumption::float,"
            " guess_consumption::float,"
            " max_consumption::float "
            "from consumptions "
            "where asset = 'eth' and price = %s "
            "order by timestamp desc "
            "limit 1",
            (int(price * 100),)
        )
        return result[0] if len(result) else None

    def get_network_power_demand(self, price: float):
        return self._run_select_query(
            "SELECT timestamp, min_power::float, guess_power::float, max_power::float "
            "FROM consumptions "
            "WHERE asset = 'eth' AND price = %s "
            "ORDER BY timestamp",
            (int(price * 100), )
        )

    def get_annualised_consumption(self, price: float):
        return self._run_select_query(
            "SELECT timestamp, min_consumption::float, guess_consumption::float, max_consumption::float "
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
        return self._run_select_query("""
            SELECT name, efficiency_gh_j AS efficiency, extract(epoch from released_at)::int AS timestamp 
            FROM cbsi_miners 
            WHERE is_active = true
            ORDER BY released_at, efficiency_gh_j
        """)

    def get_average_machine_efficiency(self):
        return self._run_select_query(
            "SELECT timestamp, value::float as efficiency, miners FROM average_machine_efficiencies "
            "WHERE asset = 'eth' ORDER BY timestamp"
        )

    def get_profitability_threshold(self, price):
        return self._run_select_query(
            "SELECT timestamp, machine_efficiency::float AS machine_efficiency "
            "FROM consumptions "
            "WHERE asset = 'eth' AND price = %s "
            "ORDER BY timestamp",
            (int(price * 100),)
        )

    def get_source_comparison(self):
        return self._run_select_query("""
            select
             label,
             value::float, 
             extract(epoch from date)::int as timestamp 
            from charts
            where name = 'eth.source_comparison' 
            order by date, label
        """)

    def get_source_comparison_for_download(self):
        return self._run_select_query("""
            select
             date,
             (max(case when label = 'CBNSI' then value end))::float as cbnsi, 
             (max(case when label = 'CCRI' then value end))::float as ccri, 
             (max(case when label = 'Kyle McDonald' then value end))::float as km, 
             (max(case when label = 'Digiconomist' then value end))::float as digiconomist 
            from charts
            where name = 'eth.source_comparison' 
            group by date
            order by date
        """)
