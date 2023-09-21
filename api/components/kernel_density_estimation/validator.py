class KernelDensityEstimationValidator:
    def __init__(self):
        self.errors = {}

    def validate(self, req_body):
        if req_body is None or 'values' not in req_body or not isinstance(req_body['values'], list):
            self.add_error('values', 'The "values" property should be array.')
        elif len(req_body['values']) == 0:
            self.add_error('values', 'The "values" should contains at least one item.')
        return not self.errors

    def add_error(self, field, error):
        if field not in self.errors:
            self.errors[field] = []
        self.errors[field].append(error)

    def get_errors(self):
        return self.errors
