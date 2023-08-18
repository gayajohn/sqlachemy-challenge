# Import the dependencies.
from concurrent.futures import ProcessPoolExecutor
from flask import Flask, jsonify

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Datetime packages
from datetime import datetime
from dateutil.relativedelta import relativedelta
import datetime as dt

# numpy package
import numpy as np


#################################################
# Database Setup
#################################################

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite", connect_args={'check_same_thread': False})

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

##################################################################
# Defining a function to convert input dates into datetime object
##################################################################

def date_converter(date):
    
    date_str = str(date).strip()
    date_dt = datetime.strptime(date_str, '%Y-%m-%d').date()

    return date_dt


##################################################
# Calculating year_ago date 
###################################################

# Find the most recent date in the data set.
recent_date_query = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

#taking the most recent data value out of the tuple, and turning it into a datetime value
recent_date = recent_date_query[0]
date = date_converter(recent_date)

# Calculate the date one year from the last date in data set.
year_ago = date - relativedelta(months=12)



#################################################
# Flask Setup
#################################################

app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def homepage():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/[start date]<br/>"
        f"/api/v1.0/[start date]/[end date]<br/>" )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the last 12 months of precipitation data"""

    # Perform a query to retrieve the data and precipitation scores and sort queries by date
    date_prcp = session.query(Measurement.date, Measurement.prcp).\
    order_by(Measurement.date).\
    filter(Measurement.date>=year_ago).all()
    
    # Making a dictionary where date is the key and prcp is the value
    date_prcp_dict={}
    for date, prcp in date_prcp:
        date_prcp_dict[date]=prcp
        
    #jsonify dictionary
    return jsonify(date_prcp_dict)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of all stations"""

    # retrieve all 9 stations
    stations_query = session.query(Station.station).all()

    # open the tuples to return a list
    stations_list = list(np.ravel(stations_query))

    #jsonify list
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Return a list of temperatures of the most active station in the last year"""

    #query to find the most active station
    most_active_station_query = session.query(Measurement.station).\
                   group_by(Measurement.station).\
                   order_by(func.count(Measurement.station).desc()).first()

    most_active_station = most_active_station_query[0]

    # query to retrieve temperature data for most active station
    active_temp = session.query(Measurement.tobs).filter(Measurement.station == most_active_station).\
              filter(Measurement.date>=year_ago).all()
    
    # turning query into list
    active_temp_list = list(np.ravel(active_temp))

    # return jsonified list
    return jsonify(active_temp_list)


@app.route("/api/v1.0/<start>")
def start_temp(start):
    """Retrieve the minimum temperature, average temperature and average temperature for all dates greater than the start date."""

    # converting input start date into datetime object
    date_dt = date_converter(start)

    # query to find min, avg and max temperature for dates after start date
    temp_query = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= date_dt).all()
    
    # turn query into list
    temp_list = list(np.ravel(temp_query))

    #if statement to check if the list is empty or not
    if temp_list[0] == None:
        return jsonify({"error": f"The date {date_dt} is not in range."}), 404

    else:
        #return jsonified list
        return jsonify(temp_list)
    
@app.route("/api/v1.0/<start>/<end>")
def start_end_temp(start, end):
    """Retrieve the minimum temperature, average temperature and average temperature for range of dates given."""

    # converting input start and end dates into datetime objects
    start_date_dt = date_converter(start)
    end_date_dt = date_converter(end)

    # Check if end date is greater than start date
    if end_date_dt >= start_date_dt:
        
        # query to find min, avg and max temperature for dates after start date
        temp_query = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start_date_dt).filter(Measurement.date <= end_date_dt).all()
        
        # turn query into list
        temp_list = list(np.ravel(temp_query))

        #if statement to check if the list is empty or not
        if temp_list[0] == None:
            return jsonify({"error": f"{start_date_dt} - {end_date_dt} is not in range."}), 404

        else:
            #return jsonified list
            return jsonify(temp_list)


    else:
    
        return jsonify({"error": f"The start date {start_date_dt} is after the end date {end_date_dt}."}), 404


if __name__ == "__main__":
    app.run(debug=True)