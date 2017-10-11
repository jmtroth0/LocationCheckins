from flask import current_app
from cassandra.query import SimpleStatement
from cassandra.policies import FallthroughRetryPolicy
import arrow
from lib.models.model_helpers import row_to_dict
from lib.models import Location


class LocationRepo(object):
    def create(self, location):
        """ Saves a location in the database
        Args:
            location: model.Location to insert into database
        """
        query = """
        INSERT INTO location (
            username,
            latitude,
            longitude,
            timestamp_created,
            location_name
        ) VALUES (
            %(username)s,
            %(latitude)s,
            %(longitude)s,
            %(timestamp_created)s,
            %(location_name)s
        )
        """
        location_dict = location.to_dict()
        client = current_app.extensions['registry']['CASSANDRA_CLIENT']
        client.execute(query, params=location_dict,
                       routing_key=location.username,
                       retry_policy=FallthroughRetryPolicy())
        query = """
        INSERT INTO location_by_timestamp (
            username,
            latitude,
            longitude,
            timestamp_created,
            location_name
        ) VALUES (
            %(username)s,
            %(latitude)s,
            %(longitude)s,
            %(timestamp_created)s,
            %(location_name)s
        )
        """
        client.execute(query, params=location_dict,
                       routing_key=location.username,
                       retry_policy=FallthroughRetryPolicy())

    def index(self):
        query = """
            SELECT * FROM location
        """
        client = current_app.extensions['registry']['CASSANDRA_CLIENT']

        results = client.execute(query)
        if not results:
            return None
        return [Location(row_to_dict(result)) for result in results]


    def get(self, username):
        query = """
            SELECT * FROM location_by_timestamp WHERE username = %(username)s
        """
        client = current_app.extensions['registry']['CASSANDRA_CLIENT']

        results = client.execute(query, params={'username': username})
        if not results:
            return None
        return [Location(row_to_dict(result)) for result in results]


