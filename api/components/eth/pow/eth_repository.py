from components.BaseRepository import CustomDataRepository
from components.gas_emission.ghg_constants import GhgConstants


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
            "SELECT timestamp, min_power::float, guess_power::float, max_power::float, "
            "min_consumption::float, guess_consumption::float, max_consumption::float "
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

    def get_greenhouse_gas_emissions(self, price):
        return self._run_select_query("""
            select name, timestamp, value from greenhouse_gas_emissions
            where asset = 'eth_pow' and price = %s
            order by timestamp, name
        """, (str(price),))

    def get_flat_greenhouse_gas_emissions(self, price):
        sql = "SELECT timestamp, to_char(MAX(DATE), 'YYYY-MM-DD HH24:MI:SS') AS date " \
              ", MAX(CASE WHEN name = ANY (ARRAY [%s, %s, %s]) THEN value END) AS min_co2 " \
              ", MAX(CASE WHEN name = ANY (ARRAY [%s, %s, %s]) THEN value END) AS guess_co2 " \
              ", MAX(CASE WHEN name = ANY (ARRAY [%s, %s, %s]) THEN value END) AS max_co2 " \
              "FROM greenhouse_gas_emissions " \
              "WHERE greenhouse_gas_emissions.asset = 'eth_pow' and greenhouse_gas_emissions.price = %s " \
              "GROUP BY greenhouse_gas_emissions.timestamp " \
              "ORDER BY timestamp"
        return self._run_select_query(sql, (
            GhgConstants.HISTORICAL_MIN_CO2, GhgConstants.MIN_CO2, GhgConstants.PREDICTED_MIN_CO2,
            GhgConstants.HISTORICAL_GUESS_CO2, GhgConstants.GUESS_CO2, GhgConstants.PREDICTED_GUESS_CO2,
            GhgConstants.HISTORICAL_MAX_CO2, GhgConstants.MAX_CO2, GhgConstants.PREDICTED_MAX_CO2,
            str(price),
        ))

    def get_total_greenhouse_gas_emissions_monthly(self, price):
        sql = """
            select date, timestamp, value, cumulative_value
            from cumulative_greenhouse_gas_emissions
            where asset = 'eth_pow' and price = %s
            order by timestamp
        """
        return self._run_select_query(sql, (str(price),))

    def get_total_greenhouse_gas_emissions_yearly(self, price):
        sql = """
            select 
                extract(year from date)::int as date,
                min(timestamp) as timestamp,
                sum(value) as value,
                max(cumulative_value) as cumulative_value
            from cumulative_greenhouse_gas_emissions
            where asset = 'eth_pow' and price = %s
            group by extract(year from date)
            order by timestamp
        """
        return self._run_select_query(sql, (str(price),))

    def get_monthly_power_mix(self):
        sql = """
            select timestamp, date, name, value
            from power_sources
            where asset = 'eth_pow' and type = 'monthly'
            order by timestamp, power_sources.order
        """
        return self._run_select_query(sql)

    def get_yearly_power_mix(self):
        sql = """
            select timestamp, date, name, value
            from power_sources
            where asset = 'eth_pow' and type = 'yearly'
            order by timestamp, power_sources.order
        """
        return self._run_select_query(sql)

    def get_emission_intensity(self):
        sql = """
            select name, co2_coef as value, timestamp, date from co2_coefficients
            where asset = 'eth_pow'
            order by timestamp
        """
        return self._run_select_query(sql)

    def get_monthly_emission_intensity(self):
        sql = """
            select min(name)                                                                                    as name,
                   min(timestamp)                                                                               as timestamp,
                   min(date)                                                                                    as date,
                   sum(co2_coef)
                       / DATE_PART('days', DATE_TRUNC('month', date) + '1 MONTH'::INTERVAL - '1 DAY'::INTERVAL) as value
            from co2_coefficients
            where asset = 'eth_pow'
            group by DATE_TRUNC('month', date)
            order by DATE_TRUNC('month', date)
           """
        return self._run_select_query(sql)
