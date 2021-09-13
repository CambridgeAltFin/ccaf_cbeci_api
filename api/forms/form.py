
from forms.validator import Validator


class Form:

    def __init__(self, data: dict):
        self.data = data
        self.errors = {}
        self.validator = Validator()
        self.before_first_error = True
        self.error_prefix = ''

    def valid(self) -> bool:
        for field, rules in self.rules().items():
            self._validate_field(field, rules)

        return len(self.errors) == 0

    def get_errors(self) -> dict:
        return self.errors

    def _validate_field(self, field: str, rules: str):
        for rule in rules.split('|'):
            rule_attrs = rule.split(':', 1)
            validation_method = getattr(self.validator, rule_attrs.pop(0), None)
            if callable(validation_method):
                error = validation_method(self.data.get(field), rule_attrs.pop() if len(rule_attrs) > 0 else None)
                if error:
                    self.push_error(field, error.format(field=field))
                    if self.before_first_error:
                        return

    def push_error(self, field: str, error: str):
        error_key = self.error_prefix + field
        if error_key not in self.errors:
            self.errors[error_key] = []
        self.errors[error_key].append(error)

    def rules(self) -> dict:
        pass
