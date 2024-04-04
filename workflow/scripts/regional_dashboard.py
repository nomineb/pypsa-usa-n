"""Dash app for exploring regional disaggregated data"""

import pypsa
from pypsa.statistics import StatisticsAccessor

from dash import Dash, html, dcc, Input, Output, callback
import plotly.express as px
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

import plotly.graph_objects as go

import logging 
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logger.propagate = False

from pathlib import Path

from typing import List, Dict

from summary import get_demand_timeseries, get_energy_timeseries
from plot_statistics import get_color_palette

###
# IDS
###

# dropdowns
DROPDOWN_SELECT_STATE = "dropdown_select_state"
DROPDOWN_SELECT_NODE = "dropdown_select_node"
DROPDOWN_SELECT_PLOT_THEME = "dropdown_select_plot_theme"

# buttons 
BUTTON_SELECT_ALL_STATES = "button_select_all_states"
BUTTON_SELECT_ALL_NODES = "button_select_all_nodes"

# slider
SLIDER_SELECT_TIME = "slider_select_time"

# radio buttons
RADIO_BUTTON_RESAMPLE = "radio_button_resample"

# graphics 
GRAPHIC_MAP = "graphic_map"
GRAPHIC_DISPATCH = "graphic_dispatch"
GRAPHIC_LOAD = "graphic_load"
GRAPHIC_SOLAR_CF = "graphic_solar_cf"
GRAPHIC_WIND_CF = "graphic_wind_cf"
GRAPHIC_EMISSIONS = "graphic_emissions"
GRAPHIC_CAPACITY = "graphic_capacity"
GRAPHIC_GENERATION = "graphic_generation"

###
# APP SETUP
###

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
logger.info("Reading configuration options")
# add logic to build network name 
network_name = "elec_s_80_ec_lv1.0_RCo2L-SAFER-RPS_E.nc"

# read in network 
network_path = Path("..", "results", "western", "networks")
NETWORK = pypsa.Network(str(Path(network_path, network_name)))
TIMEFRAME = NETWORK.snapshots

# geolocated node data structures  
ALL_STATES = NETWORK.buses.country.unique()
NODES_GEOLOCATED = NETWORK.buses
geometry = [Point(xy) for xy in zip(NODES_GEOLOCATED["x"], NODES_GEOLOCATED["y"])]
NODES_GEOLOCATED = gpd.GeoDataFrame(NODES_GEOLOCATED, geometry=geometry, crs='EPSG:4326')[["geometry"]]

# dispatch data structure 
DISPATCH = StatisticsAccessor(NETWORK).energy_balance(aggregate_time=False, aggregate_bus=False, comps=["Store", "StorageUnit", "Link", "Generator"]).mul(1e-3)
DISPATCH = DISPATCH[~(DISPATCH.index.get_level_values("carrier") == "Dc")]
DISPATCH = DISPATCH.droplevel(["component", "bus_carrier"]).reset_index()
DISPATCH["state"] = DISPATCH.bus.map(NETWORK.buses.country)
DISPATCH = DISPATCH.drop(columns="bus")
DISPATCH = DISPATCH.groupby(["carrier","state"]).sum()

# variable renewable data structure 
var_renew_carriers = [x for x in DISPATCH.index.get_level_values("carrier").unique() if (x == "Solar" or x.endswith("Wind"))]
VAR_RENEW = DISPATCH[DISPATCH.index.get_level_values("carrier").isin(var_renew_carriers)]

# load data structure 
LOAD = StatisticsAccessor(NETWORK).energy_balance(aggregate_time=False, aggregate_bus=False, comps=["Load"]).mul(1e-3).mul(-1)
LOAD = LOAD.droplevel(["component", "carrier", "bus_carrier"]).reset_index()
LOAD["state"] = LOAD.bus.map(NETWORK.buses.country)
LOAD = LOAD.drop(columns="bus")
LOAD = LOAD.groupby(["state"]).sum()

# net load data structure 
NET_LOAD = LOAD - VAR_RENEW.droplevel("carrier").groupby("state").sum()

###
# INITIALIZATION
###

logger.info("Starting app")
app = Dash(external_stylesheets=external_stylesheets)
app.title = "PyPSA-USA Dashboard"

###
# CALLBACK FUNCTIONS
### 

# select states to include 

def state_dropdown(states: List[str]) -> html.Div:
    return html.Div(
        children=[
            html.H3("States to Include"),
            dcc.Dropdown(
                id=DROPDOWN_SELECT_STATE,
                options=states,
                value=states,
                multi=True,
                persistence=True
            ),
            html.Button(
                children=["Select All"],
                id=BUTTON_SELECT_ALL_STATES,
                n_clicks=0
            )
        ]
    )

@app.callback(
    Output(DROPDOWN_SELECT_STATE, "value"),
    Input(BUTTON_SELECT_ALL_STATES, "n_clicks"),
)
def select_all_countries(_: int) -> list[str]:
    return ALL_STATES

# plot map 

def plot_map(
    n: pypsa.Network,
    states: List[str]
) -> html.Div:
    
    nodes = n.buses[n.buses.country.isin(states)]
    
    usa_map = go.Figure()

    usa_map.add_trace(go.Scattermapbox(
        mode='markers',
        lon=nodes.x,
        lat=nodes.y,
        marker=dict(size=10, color='red'),
        text=nodes.index,
    ))

    # Update layout to include map
    usa_map.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lon=-95, lat=35),
            zoom=3
        ),
        margin=dict(l=0, r=0, t=0, b=0)
    )
    
    return html.Div(children=[dcc.Graph(figure=usa_map)], id=GRAPHIC_MAP)

@app.callback(
    Output(GRAPHIC_MAP, "children"),
    Input(DROPDOWN_SELECT_STATE, "value"),
)
def plot_map_callback(
    states: list[str] = ALL_STATES,
) -> html.Div:
    return plot_map(NETWORK, states)

# plotting options 

def select_resample() -> html.Div:
    return html.Div(
        children=[
            dcc.RadioItems(
                id=RADIO_BUTTON_RESAMPLE,
                options=["1h", "2h", "4h", "24h", "W"],
                value= "1h",
                inline=True
            ),
        ]
    )

def time_slider(snapshots: pd.date_range) -> html.Div:
    return html.Div(
        children=[
            dcc.RangeSlider(
                id=SLIDER_SELECT_TIME,
                min=snapshots.min().week, 
                max=snapshots.max().week, 
                step=1, 
                value=[snapshots.min().week, snapshots.max().week],
                # marks={i:f"{int(i)}" for i in snapshots.max().week}
            )
        ]
    )

# dispatch 

def plot_dispatch(
    n: pypsa.Network,
    dispatch: pd.DataFrame,
    states: List[str],
    resample: str,
    timeframe: pd.date_range
) -> html.Div:
    
    energy_mix = dispatch[dispatch.index.get_level_values("state").isin(states)]
    energy_mix = energy_mix.droplevel("state").groupby("carrier").sum().T
    energy_mix.index = pd.to_datetime(energy_mix.index)

    energy_mix = energy_mix.loc[timeframe]
    
    energy_mix = energy_mix.resample(resample).sum()
    
    color_palette = get_color_palette(n)

    fig = px.area(
        energy_mix,
        x=energy_mix.index,
        y=energy_mix.columns,
        color_discrete_map=color_palette,
    )

    title = "Dispatch [GW]"
    fig.update_layout(
        title=dict(text=title, font=dict(size=24)),
        xaxis_title="",
        yaxis_title="Power [GW]",
    )
    
    return html.Div(children=[dcc.Graph(figure=fig)], id=GRAPHIC_DISPATCH)

@app.callback(
    Output(GRAPHIC_DISPATCH, "children"),
    Input(DROPDOWN_SELECT_STATE, "value"),
    Input(RADIO_BUTTON_RESAMPLE, "value"),
    Input(SLIDER_SELECT_TIME, "value"),
)
def plot_dispatch_callback(
    states: List[str] = ALL_STATES,
    resample: List[str] = "1h",
    weeks: List[int] = [TIMEFRAME.min().week, TIMEFRAME.max().week],
) -> html.Div:
    
    # plus one because strftime indexs from 0
    timeframe = pd.Series(TIMEFRAME.strftime("%U").astype(int) + 1, index=TIMEFRAME)
    timeframe = timeframe[timeframe.isin(range(weeks[0], weeks[-1], 1))]
    
    return plot_dispatch(NETWORK, DISPATCH, states, resample, timeframe.index)

# load 

def plot_load(
    load: pd.DataFrame,
    net_load: pd.DataFrame,
    states: List[str],
    resample: str,
    timeframe: pd.date_range
) -> html.Div:
    
    state_load = load[load.index.isin(states)].sum()
    state_net_load = net_load[net_load.index.isin(states)].sum()
    
    data = pd.concat([state_load, state_net_load], axis=1, keys=["Absolute Load", "Net Load"])
    data.index = pd.to_datetime(data.index)
    
    data = data.loc[timeframe]
    
    data = data.resample(resample).sum()

    fig = px.line(data)

    title = "System Load [GW]"
    fig.update_layout(
        title=dict(text=title, font=dict(size=24)),
        xaxis_title="",
        yaxis_title="Power [GW]",
    )
    
    return html.Div(children=[dcc.Graph(figure=fig)], id=GRAPHIC_LOAD)

@app.callback(
    Output(GRAPHIC_LOAD, "children"),
    Input(DROPDOWN_SELECT_STATE, "value"),
    Input(RADIO_BUTTON_RESAMPLE, "value"),
    Input(SLIDER_SELECT_TIME, "value"),
)
def plot_load_callback(
    states: List[str] = ALL_STATES,
    resample: List[str] = "1h",
    weeks: List[int] = [TIMEFRAME.min().week, TIMEFRAME.max().week],
) -> html.Div:
    
    # plus one because strftime indexs from 0
    timeframe = pd.Series(TIMEFRAME.strftime("%U").astype(int) + 1, index=TIMEFRAME)
    timeframe = timeframe[timeframe.isin(range(weeks[0], weeks[-1], 1))]
    
    return plot_load(LOAD, NET_LOAD, states, resample, timeframe.index)

###
# APP LAYOUT 
### 

app.layout = html.Div(
    children=[
        # map section
        html.Div(
            children = [
                html.Div(
                    children=[
                        state_dropdown(states=ALL_STATES),
                    ],
                    style={"width": "30%", "padding": "20px", "display": "inline-block", "vertical-align":"top"},
                ),
                html.Div(
                    children = [
                        plot_map_callback()
                    ],
                    style={"width": "60%", "display": "inline-block"},
                )
            ]
        ),
        # dispatch and load section
        html.Div(
            children=[
                select_resample(),
                time_slider(NETWORK.snapshots),
                plot_dispatch_callback(states = ALL_STATES, resample = "1h", weeks = [TIMEFRAME.min().week, TIMEFRAME.max().week]),
                plot_load_callback(states = ALL_STATES, resample = "1h", weeks = [TIMEFRAME.min().week, TIMEFRAME.max().week])
            ],
            style={"width": "90%", "display": "inline-block"},
        )
        
    ]
)

if __name__ == "__main__":
    app.run(debug=True)

