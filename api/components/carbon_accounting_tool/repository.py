from components.BaseRepository import CustomDataRepository


class CarbonAccountingToolRepository(CustomDataRepository):
    def get_coefficients(self, input_data):
        where_clause = ' or '.join([self._to_where_condition(x.get('from'), x.get('to')) for x in input_data])
        return self._run_select_query(f"""
            select 
                (cr."kWh_per_tx" * c2c.co2_coef / 1000) kg_per_tx,
                (cr."kWh_per_holding" * c2c.co2_coef / 1000) kg_per_holding, 
                cr.date 
            from carbon_ratings cr 
            join co2_coefficients c2c on cr.date = c2c.date
            where ({where_clause})
            order by cr.date
        """)

    def _to_where_condition(self, from_date: str, to_date=None):
        if to_date is not None:
            return f"cr.date between '{from_date}' and '{to_date}'"
        return f"cr.date = '{from_date}'"

    def get_miners_revenue(self):
        return self._run_select_query("""
            select
                label as name,
                value,
                extract(epoch from date) as timestamp
            from charts
            where name = 'carbon_accounting_tool.miners_revenue'
            order by date
        """)

    def get_miners_revenue_for_download(self):
        return self._run_select_query("""
            select
                date,
                max(case when label = 'Transaction fees' then value end) as transaction_fees,
                max(case when label = 'Mining rewards' then value end) as mining_rewards
            from charts
            where name = 'carbon_accounting_tool.miners_revenue'
            group by date
            order by date
        """)
