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
    #plot_4 = create_plot_4()
    plot_5 = create_plot_5()
    return render_template('index.html', plot_1 = plot_1, plot_top5 = plot_top5, plot_bot5 = plot_bot5, plot_2 = plot_2
    , plot_3 = plot_3
    #, plot_4 = plot_4
    , plot_5 = plot_5)




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
    df = pd.read_sql(f"select Value, Category, Date from mortality_state", con=session.connection())
    session.close()

    cat = df['Category'].unique()
    dates = df['Date'].unique()
    ymax = df['Value'].max()

    fig = go.Figure()

    if feature == 'Box':
        cnt = 1
        for i in dates:
            val = df.query(f'Date == "{i}"')['Value']
            #cat = df.query(f'Date == "{i}"')['Category']
            fig.add_trace(
                go.Box(
                    visible=False,
                    x=df.query(f'Date == "{i}"')['Category'], # assign x as the dataframe column 'x'
                    y=val,
                    name = i
                )
            )
            if cnt == len(df['Date'].unique()):
                fig.data[0].visible = True
            else:
                cnt += 1

        # fig = go.Figure(data = [[
        #     go.Box(name=i, x=df.query(f'Category == "{i}"')['Date'], y=df.query(f'Category == "{i}"')['Value']) for i in df['Category'].unique()]
        #     ,go.Box(name='All', x=df['Date'], y=df['Value'])
        #     ])
        

        fig.update_layout(title="Box Plot - All Years by Category")
    
    elif feature == 'Bar':
        cnt = 1
        for i in df['Date'].unique():
            val = df.query(f'Date == "{i}"')['Value']
            fig.add_trace(
                go.Bar(
                    visible=False,
                    x=cat, # assign x as the dataframe column 'x'
                    y=val,
                    name = i
                )
            )
            if cnt == len(df['Date'].unique()):
                fig.data[0].visible = True
            else:
                cnt += 1
        #fig = go.Figure(data = [go.Bar(name=i, x=df.query(f'Date == "{i}"')['Category'], y=df.query(f'Date == "{i}"')['Value']) for i in df['Date'].unique()])
        fig.update_layout(barmode='stack')
        fig.update_layout(title="Stacked Bar Chart - Category by Year")
    
    else:
        cnt = 1
        for i in df['Date'].unique():
            val = df.query(f'Date == "{i}"')['Value']
            fig.add_trace(
                go.Scatter(
                    visible=False,
                    x=cat, # assign x as the dataframe column 'x'
                    y=val,
                    name = i
                )
            )
            if cnt == len(df['Date'].unique()):
                fig.data[0].visible = True
            else:
                cnt += 1

        #fig = go.Figure(data = [go.Scatter(name=i, x=df.query(f'Category == "{i}"')['Date'], y=df.query(f'Category == "{i}"')['Value']) for i in df['Category'].unique()])
        fig.update_layout(title="Line Chart - Category by Year")
    
    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="restyle",
            args=["visible", [False] * len(fig.data)],
            label=fig.data[i]['name']
        )
        step["args"][1][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)
    
    sliders = [dict(
        active=0,
        currentvalue={"prefix": "Year: "},
        pad={"t": 50},
        steps=steps
    )]
    
    fig.update_layout(
        sliders=sliders
    )

    fig.update_yaxes(range=[0, ymax])
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_top5(feature):
    session = Session(engine)
    #df = pd.read_sql_table(table_name = 'mortality_us', con=session.connection(), index_col="index")
    df = pd.read_sql(f"select f.Category,f.date, f.Value from mortality_us f where rowid in (select rowid from mortality_us where Date = f.Date order by Value desc limit 6) order by f.Date asc;", con=session.connection())
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
    df = pd.read_sql(f"select f.Category,f.date, f.Value from mortality_us f where rowid in (select rowid from mortality_us where Date = f.Date order by Value asc limit 5) order by f.Date asc;", con=session.connection())
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
    
    ymax = df['Value'].max()
    
    fig = go.Figure(data = [
            go.Scatterpolar(
                theta=df['Category'], # assign x as the dataframe column 'x'
                r=df['Value']
            )
        ])
    fig.update_layout(title="Radar Chart - Category (All Years)")
    
    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="restyle",
            args=["visible", [False] * len(fig.data)],
            label=fig.data[i]['name']
        )
        step["args"][1][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)
    
    sliders = [dict(
        active=10,
        currentvalue={"prefix": "Year: "},
        pad={"t": 50},
        steps=steps
    )]
    
    fig.update_layout(
        sliders=sliders
    )

    fig.update_yaxes(range=[0, ymax])
    
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
    df = pd.read_sql(f"select Value, FIPS, Category, Date from mortality_county where Date = '2014'", con=session.connection())
    session.close()
    
    cat = df['Category'].unique()
    #dates  = df['Date'].unique()
    #data = []    
    
    fig = go.Figure()

    cnt = 1
    for c in cat:
        cat_df = df.query(f'Category == "{c}"')
        fig.add_trace( go.Choroplethmapbox(geojson=counties, locations=cat_df.FIPS, z=cat_df.Value,
                                         colorscale = "Viridis",
                                         name = c,
                                         #text =regions, 
                                         #colorbar = dict(thickness=20, ticklen=3),
                                         marker_line_width=0, marker_opacity=0.7,
                                         visible=False))
        
        if cnt == len(cat):
            fig.data[cnt-1].visible = True
        else:
            cnt += 1
       
    
    fig.update_layout(mapbox_style="carto-positron",
                    mapbox_zoom=3, mapbox_center = {"lat": 37.0902, "lon": -95.7129})
    #fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.update_layout(title="US Mortality by County - Category (2014)")
    fig.update_layout(height=600)

    button_layer_1_height = 1.12 #08
    fig.update_layout(
        updatemenus=[
            go.layout.Updatemenu(
                buttons=list([
                    dict(
                        args=["colorscale", "Viridis"],
                        label="Viridis",
                        method="restyle"
                    ),
                    dict(
                        args=["colorscale", "Cividis"],
                        label="Cividis",
                        method="restyle"
                    ),
                    dict(
                        args=["colorscale", "Blues"],
                        label="Blues",
                        method="restyle"
                    ),
                    dict(
                        args=["colorscale", "Greens"],
                        label="Greens",
                        method="restyle"
                    ),
                ]),
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.1,
                xanchor="left",
                y=button_layer_1_height,
                yanchor="top"
            ),
            go.layout.Updatemenu(
                buttons=list([
                    dict(
                        args=["reversescale", False],
                        label="False",
                        method="restyle"
                    ),
                    dict(
                        args=["reversescale", True],
                        label="True",
                        method="restyle"
                    )
                ]),
                direction="down",
                pad={"r": 10, "t": 10},
                showactive=True,
                x=0.37,
                xanchor="left",
                y=button_layer_1_height,
                yanchor="top"
            )
        ]
    )

    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="restyle",
            args=["visible", [False] * len(fig.data)],
            label=fig.data[i]['name']
        )
        step["args"][1][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)
    
    sliders = [dict(
        active=10,
        currentvalue={"prefix": "COD: "},
        pad={"t": 50},
        steps=steps        
    )]
    
    fig.update_layout(
        sliders=sliders
    )

    fig.update_layout(
        annotations=[
            go.layout.Annotation(text="colorscale", x=0, xref="paper", y=1.06, yref="paper",
                                align="left", showarrow=False),
            go.layout.Annotation(text="Reverse<br>Colorscale", x=0.25, xref="paper", y=1.07,
                                yref="paper", showarrow=False)
        ])
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON

def create_plot_5():
    session = Session(engine)
    df = pd.read_sql(f"select Value, Category, Date from mortality_us", con=session.connection())
    session.close()
    
   
    cat = df['Category'].unique()
    dates  = df['Date'].unique()
    ymax = df['Value'].max()

    fig = go.Figure()
    
    cnt = 1
    for i in df['Date'].unique():
        val = df.query(f'Date == "{i}"')['Value']
        fig.add_trace(
            go.Scatter(
                visible=False,
                x=cat, # assign x as the dataframe column 'x'
                y=val,
                name = i,
                mode='lines'
            )
        )
        if cnt == len(df['Date'].unique()):
            fig.data[cnt-1].visible = True
        else:
            cnt += 1

    steps = []
    for i in range(len(fig.data)):
        step = dict(
            method="restyle",
            args=["visible", [False] * len(fig.data)],
            label=fig.data[i]['name']
        )
        step["args"][1][i] = True  # Toggle i'th trace to "visible"
        steps.append(step)
    
    sliders = [dict(
        active=10,
        currentvalue={"prefix": "Year: "},
        pad={"t": 50},
        steps=steps
    )]
    
    fig.update_layout(
        sliders=sliders
    )

    fig.update_yaxes(range=[0, ymax])
    
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    #print(graphJSON)
    return graphJSON
    #fig.show()    



if __name__ == '__main__':
    app.run(debug=True)
