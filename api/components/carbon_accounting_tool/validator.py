from helpers import is_valid_date_string_format, daterange
from datetime import datetime


class CarbonAccountingToolValidator:
    def __init__(self):
        self.errors = {}

    def validate(self, req_body):
        if req_body is None or 'data' not in req_body or not isinstance(req_body['data'], list):
            self.add_error('data', 'The "data" property should be array.')
        elif len(req_body['data']) == 0:
            self.add_error('data', 'The "data" should contains at least one item.')
        else:
            dates = []
            for i in range(len(req_body['data'])):
                item = req_body['data'][i]
                if 'from' not in item:
                    self.add_error(f'data.{i}.from', 'The "from" value is required.')
                elif not is_valid_date_string_format(item['from']):
                    self.add_error(f'data.{i}.from', 'The "from" should be in the format "YYYY-MM-DD"')
                else:
                    if 'to' in item:
                        if not is_valid_date_string_format(item['to']):
                            self.add_error(f'data.{i}.to', 'The "to" should be in the format "YYYY-MM-DD"')
                        elif datetime.strptime(item['to'], '%Y-%m-%d') < datetime.strptime(item['from'], '%Y-%m-%d'):
                            self.add_error(f'data.{i}.to', 'The "to" should be greater than "from"')
                        else:
                            period = [
                                x.strftime('%Y-%m-%d') for x in
                                daterange(
                                    datetime.strptime(item['from'], '%Y-%m-%d'),
                                    datetime.strptime(item['to'], '%Y-%m-%d')
                                )
                            ]
                            if self.validate_period(dates, period):
                                dates += period
                            else:
                                self.add_error(f'data.{i}', 'Invalid period')
                    elif item['from'] not in dates:
                        dates.append(item['from'])
                    else:
                        self.add_error(f'data.{i}.from', 'Invalid date')
                if 'transactions' in item:
                    if not isinstance(item['transactions'], int):
                        self.add_error(f'data.{i}.transactions', 'The "transactions" should be an integer')
                    elif item['transactions'] < 0:
                        self.add_error(f'data.{i}.transactions', 'The "transactions" should be greater or equal 0')
                if 'holdings' in item:
                    if not isinstance(item['holdings'], int) and not isinstance(item['holdings'], float):
                        self.add_error(f'data.{i}.transactions', 'The "holdings" should be a number')
                    elif item['holdings'] < 0:
                        self.add_error(f'data.{i}.holdings', 'The "holdings" should be greater or equal 0')
        return not self.errors

    def add_error(self, field, error):
        if field not in self.errors:
            self.errors[field] = []
        self.errors[field].append(error)

    def get_errors(self):
        return self.errors

    def validate_period(self, dates, period):
        for date in period:
            if date in dates:
                return False
        return True
