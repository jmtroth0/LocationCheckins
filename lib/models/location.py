from base_model import Model, StringField, DateTimeField, FloatField


class Location(Model):
    username = StringField()
    location_name = StringField()
    latitude = FloatField()
    longitude = FloatField()
    timestamp_created = DateTimeField()
