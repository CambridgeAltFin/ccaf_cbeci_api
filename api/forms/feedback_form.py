
from forms.form import Form


class FeedbackForm(Form):

    def __init__(self, data: dict):
        super().__init__(data)
        self.error_prefix = 'data.'

    def rules(self) -> dict:
        return {
            'name': 'required|min:2|max:255',
            'organisation': 'min:2|max:255',
            'email': 'required|email',
            'message': 'required|min:2|max:512',
            'is_confirmed': 'required|bool|true'
        }
