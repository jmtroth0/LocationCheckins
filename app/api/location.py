from flask import (
    Blueprint, jsonify, request, render_template, current_app, redirect)
from lib.models import Location
from lib.forms import LocationForm
from lib import errors
import arrow
import requests


location = Blueprint('location', __name__)

@location.route('', methods=['POST'])
def create():
    data = request.form
    form = LocationForm.from_json(data)

    if not form.validate():
        return render_template('new.html', form=form), 400

    location_db = current_app.extensions['registry']['DB_LOCATION']

    location_data = search_location(form.location_name.data)
    if not location_data:
        form.location_name.errors = ['no location found by that name']
        return render_template('new.html', form=form), 400
    location = Location({
        'username': form.username.data,
        'location_name': form.location_name.data,
        'timestamp_created': (
            data.get('timestamp_created') or arrow.utcnow().datetime),
        'latitude': float(location_data[0]['lat']),
        'longitude': float(location_data[0]['lng'])
    })

    location_db.create(location)

    return redirect('')


@location.route('autocomplete', methods=['GET'])
def autocomplete():
    location_options = get_suggestions(request.args['value'], request.args['order'])
    return jsonify({"locations": location_options})


def search_location(location_name):
    url = ("http://api.geonames.org/searchJSON?username=dimagi&maxRows=1&name=%s"
           % location_name)
    response = requests.get(url)
    return response.json()['geonames']


def get_suggestions(location_name, orderby):
    url = ("http://api.geonames.org/searchJSON?username=dimagi&name=%s&maxRows=10&orderby=%s"
        % (location_name, orderby))
    response = requests.get(url)
    return response.json()['geonames']


@location.route('new', methods=['GET'])
def new():
    form = LocationForm()
    return render_template('new.html', form=form, locations=None)


@location.route('', methods=['GET'])
def index():
    location_db = current_app.extensions['registry']['DB_LOCATION']
    locations = location_db.index() or []

    return render_template('index.html', locations=locations)


@location.route('<username>', methods=['GET'])
def get(username):
    location_db = current_app.extensions['registry']['DB_LOCATION']
    locations = location_db.get(username) or []

    return render_template('user.html', locations=locations)
