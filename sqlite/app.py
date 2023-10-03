# Import the Flask class
from flask import Flask,jsonify
from dateutil import parser
from datetime import datetime
import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine,func

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
base = automap_base()
base.prepare(engine,reflect=True)

#getting the station and measurement tables from db
Station = base.classes.station
measurement = base.classes.measurement

session = Session(engine)

# Create an instance of the Flask class
app = Flask(__name__)


def list_routes():
    """ function to list all available routes """
    routes = []
    for rule in app.url_map.iter_rules():
        route = {
            'methods': ','.join(rule.methods),
            'path': str(rule),
        }
        routes.append(route)
    return routes

@app.route('/', methods=['GET'])
def get_routes():
    """api to list all routes"""
    return {'routes': list_routes()}

@app.route('/api/v1.0/precipitation',methods=['GET'])
def precipitation():
    """api to list of precipitation of previous year """
    recent_date = parser.parse(session.query(func.max(measurement.date)).scalar())
    twelve_month_period = recent_date - timedelta(days=365)
    last_twelve_month_data = session.query(measurement).filter(measurement.date >= twelve_month_period).all()
    final_data_list = [{"id":item.id,
                        "prcp":item.prcp,
                        "tobs":item.tobs,
                        "station":item.station,
                        "date":item.date} for item in last_twelve_month_data]
    return jsonify(final_data_list)
    
@app.route('/api/v1.0/stations',methods=["GET"])
def station():
    """ api to list all stations """
    station_result= session.query(Station).all()
    station_list =[{"id":item.id,
                    "station":item.station,
                    "name":item.name} for item in station_result]
    return jsonify(station_list)

@app.route('/api/v1.0/tobs',methods=["GET"])
def tobs():
    """ api to list temparature of most active station """
    recent_date = parser.parse(session.query(func.max(measurement.date)).scalar())
    twelve_month_period = recent_date - timedelta(days=365)
    
    active_stations = (
    session.query(Station.station, func.count(measurement.station).label('entry_count'))
    .join(measurement, Station.station == measurement.station)
    .group_by(Station.station)
    .order_by(func.count(measurement.station).desc())
    .all()
    )
    most_active_station = active_stations[0][0]    
    # Query the dates and temperature observations of the most-active station for the previous year of data.
    temp_date_active = (session.query(measurement.tobs,
                                     measurement.date)
                            .filter(measurement.station == most_active_station)
                            .filter(measurement.date >= twelve_month_period).all())

    return jsonify([{"tobs":str(item[0]),
                    "date":str(item[1])} for item in temp_date_active])

@app.route('/api/v1.0/<start>',methods=["GET"])
def mean_average_max_temp(start):
    """ api to fetch temparature greater than specified date
        sample_input: http://127.0.0.1:5000/api/v1.0/2016-10-21
    """
    result = (
    session.query(
        measurement.station.label('station'),
        func.min(measurement.tobs).label('min_tobs'),
        func.max(measurement.tobs).label('max_tobs'),
        func.avg(measurement.tobs).label('mean_tobs')
    )
    .filter(measurement.date >= start)
    .group_by(measurement.station)
    .all()
    )
    return jsonify([{"station":item.station,
                     "max":item.max_tobs,
                     "min":item.min_tobs,
                     "average":item.mean_tobs} for item in result])


@app.route('/api/v1.0/<start>/<end>',methods=["GET"])
def temp_start_end(start,end):
    """ api to fetch temparature between a range
        sample_input: http://127.0.0.1:5000/api/v1.0/2016-08-28/2016-10-22
    """
    result = (
    session.query(
        measurement.station.label('station'),
        func.min(measurement.tobs).label('min_tobs'),
        func.max(measurement.tobs).label('max_tobs'),
        func.avg(measurement.tobs).label('mean_tobs')
    )
    .filter(measurement.date >= start)
    .filter(measurement.date <= end)
    .group_by(measurement.station)
    .all()
    )
    return jsonify([{"station":item.station,
                     "max":item.max_tobs,
                     "min":item.min_tobs,
                     "average":item.mean_tobs} for item in result])

# Run the application if this script is executed
if __name__ == '__main__':
    app.run(debug=True)