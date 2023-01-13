import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify 


# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect= True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all daily precipitation totals for the last 12 month of data"""
    # Query and summarize daily precipitation across all stations for the last year of available data
    
    start_date = '2016-08-23'
    precipitation_data = session.query(Measurement.date, func.sum(Measurement.prcp)).\
                        filter(Measurement.date >= start_day) .\
                        group_by(Measurement.date).\
                        order_by(Measurement.date).all()
   
    session.close()


    # Return a dictionary with the date as key and the daily precipitation total as value
    precipitation_dates = []
    precipitation_totals = []
    
    for date, total in precipitation_data:
        precipitation_dates.append(date)
        precipitation_totals.append(total)
    
    precipitation_dict = dict(zip(precipitation_dates, precipitation_totals))

    
    return jsonify(precipitation_dict)




@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a JSON list of stations from the dataset."""
    # Return a list of active weather stations in Hawaii
    active_stations = session.query(Measurement.station).\
        group_by(Measurement.station).all()
    
    session.close()


    # Convert list of tuples into normal list and return the JSonified list
    list_of_stations = list(np.ravel(active_stations)) 
    return jsonify(list_of_stations)




@app.route("/api/v1.0/tobs")
def tobs():
    
    """ Query the last 12 months of temperature observation data for the most active station"""
    session = Session(engine)
    active_stations = session.query(Measurement.station, func.count(Measurement.id)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.id).desc()).all()
    most_active_station = active_stations[0][0]

    session.close()
    most_active_station = active_stations[0][0]
    
    session = Session(engine)
    most_active_station_end_day = session.query(Measurement.date,Measurement.tobs).\
        filter(Measurement.station == most_active_station).\
        order_by(Measurement.date.desc()).first()
    session.close()
    
    last_day_station1 = dt.datetime.strptime(most_active_station_end_day[0], '%Y-%m-%d')
    start_day_station1 = last_day_station1 - dt.timedelta(days=366)
    
    session = Session(engine)
    station1_tobs = session.query(Measurement.date,Measurement.tobs).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= start_day_station1).all()
    session.close()
    
    
    
    
    
    
    # Return a dictionary with the date as key and the daily temperature observation as value
    observation_dates = []
    temperature_observations = []

    for date, observation in station1_tobs:
        observation_dates.append(date)
        temperature_observations.append(observation)
    
    most_active_tobs_dict = dict(zip(observation_dates, temperature_observations))

    return jsonify(most_active_tobs_dict)




@app.route("/api/v1.0/trip/<start_date>")
def start_with(start_date):
    
    """
    Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start date.
    If no end date is provided, the function defaults to 2017-08-23.
    """
    session = Session(engine)
    summary_temperature = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()
    session.close()

    start_with_list = []
    for min, avg, max in summary_temperature:
        start_with_dict = {}
        start_with_dict["Min"] = min
        start_with_dict["Average"] = avg
        start_with_dict["Max"] = max
        start_with_list.append(start_with_dict)

    return jsonify(start_with_list)
  


@app.route("/api/v1.0/trip/<start_date>/<end_date>")
def start_end(start_date, end_date='2017-08-23'):
    
    """
    Calculate minimum, average and maximum temperatures for the range of dates starting with start date.
    If no valid end date is provided, the function defaults to 2017-08-23.
    """
    
    session = Session(engine)
    summary_temperature = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).\
        filter(Measurement.date <= end_date).all()
    session.close()

    start_end_list = []
    for min, avg, max in summary_temperature:
        start_end_dict = {}
        start_end_dict["Min"] = min
        start_end_dict["Average"] = avg
        start_end_dict["Max"] = max
        start_end_list.append(start_end_dict)
        
    return jsonify(start_end_list)


if __name__ == '__main__':
    app.run(debug=True)
