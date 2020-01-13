import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///assets/data/mortality.db")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)


# Save reference to the table
mortality_county = Base.classes.mortality_county
mortality_state = Base.classes.mortality_state
mortality_us = Base.classes.mortality_us
#mortality_categories = Base.classes.vw_Category

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f'<a href="/api/v1.0/categories">/api/v1.0/categories</a></br>'
        f'<a href="/api/v1.0/years">/api/v1.0/years</a></br>'
        f'<a href="/api/v1.0/county_all">/api/v1.0/county_all</a></br>'  
        f'<a href="/api/v1.0/county_year/2000">/api/v1.0/county_year/&ltyear&gt</a></br>'
        f'<a href="/api/v1.0/state_year/2000">/api/v1.0/state_year/&ltyear&gt</a></br>'
        f'<a href="/api/v1.0/us_year/2000">/api/v1.0/us_year/&ltyear&gt</a></br>'
    )

@app.route("/api/v1.0/categories")
def categories():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all Category names"""
    # Query all passengers
    results = session.query(mortality_us.Category).distinct().all()

    session.close()

    # Convert list of tuples into normal list
    all_categories = list(np.ravel(results))

    return jsonify(all_categories)

@app.route("/api/v1.0/years")
def years():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all Category names"""
    # Query all passengers
    results = session.query(mortality_us.Date).distinct().all()

    session.close()

    # Convert list of tuples into normal list
    all_categories = list(np.ravel(results))

    return jsonify(all_categories)


@app.route("/api/v1.0/county_all")
def county_all():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(mortality_county.FIPS, mortality_county.Category, mortality_county.Date, mortality_county.Value).all()

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_data = []
    for FIPS, Category, Date, Value in results:
        mortality_dict = {}
        mortality_dict["FIPS"] = FIPS
        mortality_dict["Category"] = Category
        mortality_dict["Date"] = Date
        mortality_dict["Value"] = Value
        all_data.append(mortality_dict)

    return jsonify(all_data)

@app.route("/api/v1.0/county_year/<year>")
def county_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(mortality_county.FIPS, mortality_county.Category, mortality_county.Date, mortality_county.Value).\
        filter(mortality_county.Date == year).all()

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_data = []
    for FIPS, Category, Date, Value in results:
        mortality_dict = {}
        mortality_dict["FIPS"] = FIPS
        mortality_dict["Category"] = Category
        mortality_dict["Date"] = Date
        mortality_dict["Value"] = Value
        all_data.append(mortality_dict)

    return jsonify(all_data)

@app.route("/api/v1.0/state_year/<year>")
def state_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(mortality_state.FIPS, mortality_state.Category, mortality_state.Date, mortality_state.Value).\
        filter(mortality_state.Date == year).all()

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_data = []
    for FIPS, Category, Date, Value in results:
        mortality_dict = {}
        mortality_dict["FIPS"] = FIPS
        mortality_dict["Category"] = Category
        mortality_dict["Date"] = Date
        mortality_dict["Value"] = Value
        all_data.append(mortality_dict)

    return jsonify(all_data)
    
@app.route("/api/v1.0/us_year/<year>")
def us_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all passengers
    results = session.query(mortality_us.Category, mortality_us.Date, mortality_us.Value).\
        filter(mortality_us.Date == year).all()

    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_data = []
    for Category, Date, Value in results:
        mortality_dict = {}
        mortality_dict["Category"] = Category
        mortality_dict["Date"] = Date
        mortality_dict["Value"] = Value
        all_data.append(mortality_dict)

    return jsonify(all_data)

if __name__ == '__main__':
    app.run(debug=True)
