# Import dependacies
import numpy as np
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

# Database Setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save reference to the table
Station = Base.classes.station
Measurement = Base.classes.measurement

# Flask Setup
app = Flask(__name__)

# Flask Routes
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"

    )


@app.route("/api/v1.0/precipitation")
def prcp():
    # Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
    #create session
    session = Session(engine)
    # Query all dates and prcps
    results_prcp = session.query(Measurement.date, Measurement.prcp).all()
    session.close()

    # Convert list normal list
    Prcp_data =[]
    for date, prcp in results_prcp:
        prcp_dict = {}
        prcp_dict["date"] = date
        prcp_dict["prcp"] = prcp
        Prcp_data.append(prcp_dict)
    return jsonify(Prcp_data)


@app.route("/api/v1.0/stations")
def staions():
    # Return a JSON list of stations from the dataset.
    #create session
    session = Session(engine)
    # Query all stations
    results_stations = session.query(Station.name).all()
    session.close()

    # Convert list of tuples into normal 
    Stations_list = list(np.ravel(results_stations))

    return jsonify(Stations_list)


@app.route("/api/v1.0/tobs")
def tobs():
    # Return a JSON list of stations from the dataset.
    #create session
    session = Session(engine)

    # setting end and start dates
    xy = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    xy = xy[0]
    end_date = pd.to_datetime(xy).date()
    start_date = end_date - pd.DateOffset(years=1)
    start_date = pd.to_datetime(start_date).date()
    # Setting station with max records
    station_usage = engine.execute('''
    SELECT s.'station', COUNT(m.'id') as measurments
    FROM station s, measurement m WHERE m.'station' = s.'station'
    GROUP BY s.'station'
    ORDER BY measurments desc''').fetchall()

    Station_usage_df = pd.DataFrame(station_usage, columns=["Station", "Entries"])
    Max_used_station = Station_usage_df['Station'].head(1)
    Max_used_station = Max_used_station[0]  

    # Query date and tobs for dates and stations specified above
    results_tobs =  session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).filter(Measurement.station == Max_used_station).all()
    session.close()

    # convert row data into a dictionary
    tobs_query=[]
    for date, tobs in results_tobs:
        tobs_dict = {}
        tobs_dict["date"] = date
        tobs_dict["tobs"] = tobs
        tobs_query.append(tobs_dict)

    return jsonify(tobs_query)


@app.route("/api/v1.0/<start>")
def weather_stats(start):
    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    # create session
    session = Session(engine)
    # formatting specified dates
    start = datetime.strptime(start, "%Y%m%d").date()
    #creating unpacking func
    sel = [func.min(Measurement.tobs).label('Min_Temperature'), func.avg(Measurement.tobs).label('Avg_Temperature'), func.max(Measurement.tobs).label('Max_Temperature')]
    
 
    results_weather_stats = session.query(*sel).\
        filter(Measurement.date >= start).all()
    
    #close session
    session.close()

  
    #create list and a dict from the row data from the query
    weather_stats = []
    for Min_Temperature, Avg_Temperature, Max_Temperature in results_weather_stats:
        stats_dict = {}
        stats_dict['Min_Temperature'] = Min_Temperature
        stats_dict['Avg_Temperature'] = Avg_Temperature
        stats_dict['Max_Temperature'] = Max_Temperature
        weather_stats.append(stats_dict)
    
    return jsonify(temps=weather_stats)


@app.route("/api/v1.0/<start>/<end>")
def weather_stats2(start, end):
    # Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    # create session
    session = Session(engine)
    # formatting specified dates
    start = datetime.strptime(start, "%Y%m%d").date()
    end = datetime.strptime(end, "%Y%m%d").date()
    sel = [func.min(Measurement.tobs).label('Min_Temperature'), func.avg(Measurement.tobs).label(
        'Avg_Temperature'), func.max(Measurement.tobs).label('Max_Temperature')]

    #query stats from specified date to current
    results_weather_stats = session.query(*sel).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).all()
    #close session
    session.close()
    #create list and a dict from the row data from the query
    weather_stats = []
    for Min_Temperature, Avg_Temperature, Max_Temperature in results_weather_stats:
        stats_dict = {}
        stats_dict['Min_Temperature'] = Min_Temperature
        stats_dict['Avg_Temperature'] = Avg_Temperature
        stats_dict['Max_Temperature'] = Max_Temperature
        weather_stats.append(stats_dict)
    #return data in json format
    return jsonify(weather_stats)

if __name__ == "__main__":
    app.run(debug=True)
