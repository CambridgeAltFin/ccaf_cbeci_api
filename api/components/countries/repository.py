from components.BaseRepository import CustomDataRepository


class CountryRepository(CustomDataRepository):

    def all(self):
        return self._run_select_query(
            'SELECT country, code, electricity_consumption, country_flag '
            'FROM countries '
            'WHERE electricity_consumption IS NOT NULL '
            'ORDER BY electricity_consumption DESC'
        )

    def update_by_code(self, code, value):
        sql = 'UPDATE countries SET electricity_consumption = %s WHERE code = %s'
        self._run_query(sql, (value, code))
