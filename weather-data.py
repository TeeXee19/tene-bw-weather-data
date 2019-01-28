import atexit
import datetime
import logging
import urllib2

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, json, request
from flask_pymongo import PyMongo

app = Flask(__name__)

# url to get data from (append the location key to the url)
WEATHER_SOURCE_URL = 'http://dataservice.accuweather.com/currentconditions/v1/'
# Accuweather api key for getting weather data
# This key is restricted to 50 calls per 24hrs
API_KEY = 'lhRjDpUWkD5NNYGVKfbxinlqDt3WpasZ'
TIME_IN_SECONDS = 3600
# initialise db
weatherdata_DB = PyMongo(app)

# TODO Move the airports records to file
airports = [
    {
        'name': 'Murtala Mohammed International Airport',
        'key': '253768',
        'location': 'Lagos'},
    {
        'name': 'Akwa Ibom International Airport',
        'key': '251987',
        'location': 'Akwa Ibom'
    },
    {
        'name': 'Nnamdi Azikiwe International Airport',
        'key': '251708',
        'location': 'Abuja'
    },
    {
        'name': 'Port Harcourt International Airport',
        'key': '252012',
        'location': 'Port Harcourt'
    }
]

# setup logging for appscheduler
log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.INFO)  # DEBUG
fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)


def getWeatherData():
    with app.app_context():
        global WEATHER_SOURCE_URL
        global API_KEY
        global weatherdata_DB
        global airports
        for airport in airports:
            url = WEATHER_SOURCE_URL + airport.get('key') + "?apikey=" + API_KEY
            try:
                response = urllib2.urlopen(url)
            except urllib2.HTTPError as e:
                print jsonify({'status': e.code, 'message': 'The server couldn\'t fulfill the request'})
                return
            except urllib2.URLError as e:
                print jsonify({'status': 500, 'message': e.reason})
                return
            else:
                weatherData = json.loads(response.read())[0]

                airports_weather_db = weatherdata_DB.db.airpots_weather_db
                airports_weather_db.insert({
                    'name': airport.get('name'),
                    'location': airport.get('location'),
                    'key': airport.get('key'),
                    "localObservationDateTime": weatherData['LocalObservationDateTime'],
                    "epochTime": weatherData['EpochTime'],
                    "weatherText": weatherData['WeatherText'],
                    "temperature": weatherData['Temperature']['Metric'],
                    'time_fetched': datetime.datetime.now()
                })


scheduler = BackgroundScheduler()
scheduler.start()
scheduler.add_job(
    func=getWeatherData,
    trigger=IntervalTrigger(seconds=TIME_IN_SECONDS),
    id='weather_job',
    name='Get all weather data',
    replace_existing=True)
# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())


@app.route('/weather/airport/', methods=['POST'])
def getAirportWeather():
    keys_json = request.get_json()
    airport_keys = keys_json['keys']
    airports_data_db = weatherdata_DB.db.airpots_weather_db
    # airport_keys = json.load(request.get_json())['keys']
    # return jsonify(airport_keys)
    airport_data_list = []
    for airport_key in airport_keys:
        airport_data_cursor = airports_data_db.find({'key': str(airport_key)}).sort('_id', 1).limit(1)
        for airport_data in airport_data_cursor:
            airport_data_list.append({
                'name': airport_data['name'],
                'key': airport_data['key'],
                'location': airport_data['location'],
                "localObservationDateTime": airport_data['localObservationDateTime'],
                "weatherText": airport_data['weatherText'],
                "temperature": airport_data['temperature']
            })
    return jsonify({'status': 200, 'result': airport_data_list})


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
