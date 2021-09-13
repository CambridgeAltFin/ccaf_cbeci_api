

class Validator:

    def required(self, value, attr: None) -> bool or str:
        return False if value is not None and value != '' else 'This field is required'

    def email(self, value, attr: None) -> bool or str:
        split_result = value.split('@')

        if len(split_result) != 2 or len(split_result[1].split('.')) != 2:
            return 'Must be a valid email'
        return False

    def min(self, value, attr: str) -> bool or str:
        if type(value) is str and value and len(value) < int(attr):
            return 'Must be at least ' + attr + ' characters long'
        return False

    def max(self, value, attr: str) -> bool or str:
        if type(value) is str and value and len(value) > int(attr):
            return 'Must be less than or equal to ' + str(attr) + ' characters long'
        return False

    def bool(self, value, attr: None) -> bool or str:
        return False if type(value) is bool else '"{field}" must be boolean'

    def true(self, value, attr: None) -> bool or str:
        return False if value is True else '"{field}" must be [true]'

