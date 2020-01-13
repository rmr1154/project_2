import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify, render_template, redirect, request
from etl import * 
import plotly
import plotly.graph_objs as go


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
def index():
    feature = 'Bar'
    bar = create_plot(feature)
    return render_template('index.html', plot=bar)


@app.route("/etl")
@app.route("/etl/")
def etl():
    #return process_etl()
     try:
         process_etl()
         return redirect("/", code=302)
     except:
         return 'Something went horribly wrong!'
    


@app.route("/api/")
@app.route("/api")
def api():
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

@app.route("/api/v1.0/categories/")
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

@app.route("/api/v1.0/years/")
@app.route("/api/v1.0/years")
def years():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    results = session.query(mortality_us.Date).distinct().all()

    session.close()

    # Convert list of tuples into normal list
    all_categories = list(np.ravel(results))

    return jsonify(all_categories)

@app.route("/api/v1.0/county_all/")
@app.route("/api/v1.0/county_all")
def county_all():
    # Create our session (link) from Python to the DB
    session = Session(engine)

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

@app.route("/api/v1.0/county_year/<year>/")
@app.route("/api/v1.0/county_year/<year>")
def county_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

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

@app.route("/api/v1.0/state_year/<year>/")
@app.route("/api/v1.0/state_year/<year>")
def state_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

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

@app.route("/api/v1.0/us_year/<year>/")    
@app.route("/api/v1.0/us_year/<year>")
def us_year(year):
        # Create our session (link) from Python to the DB
    session = Session(engine)

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

def create_plot(feature):
    if feature == 'Bar':
        N = 40
        x = np.linspace(0, 1, N)
        y = np.random.randn(N)
        df = pd.DataFrame({'x': x, 'y': y}) # creating a sample dataframe
        data = [
            go.Bar(
                x=df['x'], # assign x as the dataframe column 'x'
                y=df['y']
            )
        ]
    else:
        N = 1000
        random_x = np.random.randn(N)
        random_y = np.random.randn(N)

        # Create a trace
        data = [go.Scatter(
            x = random_x,
            y = random_y,
            mode = 'markers'
        )]


    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

@app.route('/bar', methods=['GET', 'POST'])
def change_features():

    feature = request.args['selected']
    graphJSON= create_plot(feature)

    return graphJSON    

if __name__ == '__main__':
    app.run(debug=True)
