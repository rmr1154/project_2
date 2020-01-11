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
        f"/api/v1.0/categories<br/>"
        f"/api/v1.0/passengers"
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


@app.route("/api/v1.0/county")
def county():
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


if __name__ == '__main__':
    app.run(debug=True)
