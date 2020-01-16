import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.inspection import inspect
from flask import Flask, jsonify, render_template, redirect, request
from etl import * 
import plotly
import plotly.graph_objs as go
from collections import defaultdict
from urllib.request import urlopen
import json


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
    plot_1 = create_plot_1(feature)
    plot_top5 = create_plot_top5(feature)
    plot_bot5 = create_plot_bot5(feature)
    plot_2 = create_plot_2()
    plot_3 = create_plot_3()
    plot_4 = create_plot_4()
    plot_5 = create_plot_5()
    return render_template('index.html', plot_1 = plot_1, plot_top5 = plot_top5, plot_bot5 = plot_bot5, plot_2 = plot_2, plot_3 = plot_3, plot_4 = plot_4, plot_5 = plot_5)




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
        f'<hr>'
        f'<a href="/api/v1.0/categories">/api/v1.0/categories</a></br>'
        f'<a href="/api/v1.0/years">/api/v1.0/years</a></br>'
        f'<a href="/api/v1.0/county_all">/api/v1.0/county_all</a></br>'  
        f'<a href="/api/v1.0/county_year/2000">/api/v1.0/county_year/&ltyear&gt</a></br>'
        f'<a href="/api/v1.0/state_year/2000">/api/v1.0/state_year/&ltyear&gt</a></br>'
        f'<a href="/api/v1.0/us_year/2000">/api/v1.0/us_year/&ltyear&gt</a></br>'
        f'</br>'
        f'<hr>'
        f'<a href="/">Return to the Dashboard</a>'

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

@app.route('/bar', methods=['GET', 'POST'])
def change_features():

    feature = request.args['selected']
    graphJSON = create_plot_1(feature)
    
    return graphJSON    

@app.route('/bar2', methods=['GET', 'POST'])
def change_features2():

    feature = request.args['selected']
    graphJSON = create_plot_top5(feature)
    
    return graphJSON   

@app.route('/bar3', methods=['GET', 'POST'])
def change_features3():

    feature = request.args['selected']
    graphJSON = create_plot_bot5(feature)
    
    return graphJSON   

def create_plot_1(feature):
    session = Session(engine)
    #df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    df = pd.read_sql(f"select sum(value) as Value, Category, Date from mortality_us group by Category, Date ", con=session.connection())
    session.close()

    if feature == 'Box':

        fig = go.Figure(data = [
            go.Box(
                x=df['Category'], # assign x as the dataframe column 'x'
                y=df['Value']
            )
        ])
        fig.update_layout(title="Box Plot - All Years by Category")
    
    elif feature == 'Bar':

        fig = go.Figure(data = [go.Bar(name=i, x=df.query(f'Date == "{i}"')['Category'], y=df.query(f'Date == "{i}"')['Value']) for i in df['Date'].unique()])
        fig.update_layout(barmode='stack')
        fig.update_layout(title="Stacked Bar Chart - Category by Year")
    
    else:

        fig = go.Figure(data = [go.Scatter(name=i, x=df.query(f'Category == "{i}"')['Date'], y=df.query(f'Category == "{i}"')['Value']) for i in df['Category'].unique()])
        fig.update_layout(title="Line Chart - Category by Year")
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_top5(feature):
    session = Session(engine)
    #df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    df = pd.read_sql(f"select f.Category,f.date, f.Value from mortality_us f where rowid in (select rowid from mortality_us where Date = f.Date order by Value desc limit 6) order by f.Value desc,f.Date asc;", con=session.connection())
    session.close()

    if feature == 'Box':

        fig = go.Figure(data = [
            go.Box(
                x=df['Category'], # assign x as the dataframe column 'x'
                y=df['Value']
            )
        ])
        fig.update_layout(title="Box Plot - Top 6 - All Years by Category")
    
    elif feature == 'Bar':

        fig = go.Figure(data = [go.Bar(name=i, x=df.query(f'Date == "{i}"')['Category'], y=df.query(f'Date == "{i}"')['Value']) for i in df['Date'].unique()])
        fig.update_layout(barmode='stack')
        fig.update_layout(title="Stacked Bar Chart (Top 6) - Category by Year")
    
    else:

        fig = go.Figure(data = [go.Scatter(name=i, x=df.query(f'Category == "{i}"')['Date'], y=df.query(f'Category == "{i}"')['Value']) for i in df['Category'].unique()])
        fig.update_layout(title="Line Chart (Top 6) - Category by Year")
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_bot5(feature):
    session = Session(engine)
    #df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    df = pd.read_sql(f"select f.Category,f.date, f.Value from mortality_us f where rowid in (select rowid from mortality_us where Date = f.Date order by Value asc limit 5) order by f.Value asc,f.Date asc;", con=session.connection())
    session.close()

    if feature == 'Box':

        fig = go.Figure(data = [
            go.Box(
                x=df['Category'], # assign x as the dataframe column 'x'
                y=df['Value']
            )
        ])
        fig.update_layout(title="Box Plot - Top 6 - All Years by Category")
    
    elif feature == 'Bar':

        fig = go.Figure(data = [go.Bar(name=i, x=df.query(f'Date == "{i}"')['Category'], y=df.query(f'Date == "{i}"')['Value']) for i in df['Date'].unique()])
        fig.update_layout(barmode='stack')
        fig.update_layout(title="Stacked Bar Chart (Top 6) - Category by Year")
    
    else:

        fig = go.Figure(data = [go.Scatter(name=i, x=df.query(f'Category == "{i}"')['Date'], y=df.query(f'Category == "{i}"')['Value']) for i in df['Category'].unique()])
        fig.update_layout(title="Line Chart (Top 6) - Category by Year")
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_2():
    session = Session(engine)
    df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    session.close()

    fig = go.Figure(data = [
            go.Scatterpolar(
                theta=df['Category'], # assign x as the dataframe column 'x'
                r=df['Value']
            )
        ])
    fig.update_layout(title="Radar Chart - Category (All Years)")
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_3():
    session = Session(engine)
    df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    session.close()

    fig = go.Figure(data = [
            go.Pie(
                labels=df['Category'], # assign x as the dataframe column 'x'
                values=df['Value']
            )
        ])
    fig.update_layout(title="Pie Chart - Category (All Years)")

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_4():
    with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
        counties = json.load(response)

    session = Session(engine)
    #df = pd.read_sql_table(table_name = 'mortality_county', con=session.connection(), index_col="index")
    #df = pd.read_sql(f"select * from mortality_county where Date = '2014' and Category = 'Diabetes'", con=session.connection(), index_col="index")
    df = pd.read_sql(f"select sum(value) as Value, FIPS from mortality_county where Date = '2014' and Category = 'Mental disorders' group by FIPS", con=session.connection())
    session.close()

    colorscale = ["#f7fbff","#ebf3fb","#deebf7","#d2e3f3","#c6dbef","#b3d2e9","#9ecae1",
              "#85bcdb","#6baed6","#57a0ce","#4292c6","#3082be","#2171b5","#1361a9",
              "#08519c","#0b4083","#08306b"]

    fig = go.Figure(go.Choroplethmapbox(geojson=counties, locations=df.FIPS, z=df.Value,
                                        #colorscale=colorscale, #"Viridis", #zmin=0, zmax=50,
                                        colorscale="Viridis", #zmin=0, zmax=50,
                                        marker_opacity=0.5, marker_line_width=0))
    fig.update_layout(mapbox_style="carto-positron",
                    mapbox_zoom=3, mapbox_center = {"lat": 37.0902, "lon": -95.7129})
    #fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.update_layout(title="US Mortality by County - Category (All Years)")
    fig.update_layout(height=600)
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_5():

    session = Session(engine)
    df = pd.read_sql_table(table_name = 'mortality_state', con=session.connection(), index_col="index")
    session.close()

    data = []

    layout = dict(
        title = 'State Mortality per year 1985-2014<br>',
        # showlegend = False,
        autosize = False,
        width = 1000,
        height = 900,
        hovermode = False,
        legend = dict(
            x=0.7,
            y=-0.1,
            bgcolor="rgba(255, 255, 255, 0)",
            font = dict( size=11 ),
        )
    )
    years = df['Date'].unique()

    for i in range(len(years)):
        geo_key = 'geo'+str(i+1) if i != 0 else 'geo'
        lons = list(df[ df['Date'] == years[i] ]['Lon'])
        lats = list(df[ df['Date'] == years[i] ]['Lat'])
        # Walmart store data
        data.append(
            dict(
                type = 'scattergeo',
                showlegend=False,
                lon = lons,
                lat = lats,
                geo = geo_key,
                name = int(years[i]),
                marker = dict(
                    color = "rgb(0, 0, 255)",
                    opacity = 0.5
                )
            )
        )
        # Year markers
        data.append(
            dict(
                type = 'scattergeo',
                showlegend = False,
                lon = [-78],
                lat = [47],
                geo = geo_key,
                text = [years[i]],
                mode = 'text',
            )
        )
        layout[geo_key] = dict(
            scope = 'usa',
            showland = True,
            landcolor = 'rgb(229, 229, 229)',
            showcountries = False,
            domain = dict( x = [], y = [] ),
            subunitcolor = "rgb(255, 255, 255)",
        )


    def draw_sparkline( domain, lataxis, lonaxis ):
        ''' Returns a sparkline layout object for geo coordinates  '''
        return dict(
            showland = False,
            showframe = False,
            showcountries = False,
            showcoastlines = False,
            domain = domain,
            lataxis = lataxis,
            lonaxis = lonaxis,
            bgcolor = 'rgba(255,200,200,0.0)'
        )

    # Stores per year sparkline
    layout['geo44'] = draw_sparkline({'x':[0.6,0.8], 'y':[0,0.15]}, \
                                    {'range':[-5.0, 30.0]}, {'range':[0.0, 40.0]} )
    data.append(
        dict(
            type = 'scattergeo',
            mode = 'lines',
            lat = list(df.groupby(by=['Date']).count()['Category']/1e1),
            lon = list(range(len(df.groupby(by=['Date']).count()['Category']/1e1))),
            line = dict( color = "rgb(0, 0, 255)" ),
            name = "New stores per year<br>Peak of 178 stores per year in 1990",
            geo = 'geo44',
        )
    )

    # Cumulative sum sparkline
    layout['geo45'] = draw_sparkline({'x':[0.8,1], 'y':[0,0.15]}, \
                                    {'range':[-5.0, 50.0]}, {'range':[0.0, 50.0]} )
    data.append(
        dict(
            type = 'scattergeo',
            mode = 'lines',
            lat = list(df.groupby(by=['Date']).count().cumsum()['Category']/1e2),
            lon = list(range(len(df.groupby(by=['Date']).count()['Category']/1e1))),
            line = dict( color = "rgb(214, 39, 40)" ),
            name ="Cumulative sum<br>3176 stores total in 2006",
            geo = 'geo45',
        )
    )

    z = 0
    COLS = 4
    ROWS = 2
    for y in reversed(range(ROWS)):
        for x in range(COLS):
            geo_key = 'geo'+str(z+1) if z != 0 else 'geo'
            layout[geo_key]['domain']['x'] = [float(x)/float(COLS), float(x+1)/float(COLS)]
            layout[geo_key]['domain']['y'] = [float(y)/float(ROWS), float(y+1)/float(ROWS)]
            z=z+1
            if z > 42:
                break

    fig = go.Figure(data=data, layout=layout)
    fig.update_layout(width=800)
    fig.update_layout(title="Mortality by State Over Time")

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON
    #fig.show()    




if __name__ == '__main__':
    app.run(debug=True)
