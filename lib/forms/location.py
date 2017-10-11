from wtforms import Form, StringField, validators


class LocationForm(Form):
    username = StringField('username', [validators.InputRequired()])
    location_name = StringField('location name', [validators.InputRequired()])
