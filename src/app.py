import plotly.graph_objects as go # or plotly.express as px
from dash import Dash, dcc, html, Input, Output, State, callback, dash_table,ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import plotly.express as px
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import os


data=pd.read_parquet("../data/grouped_data")#,                     filters=[('type', '>=', 80),('type', '<', 90)])
   
tracking=pd.read_parquet("../data/track_comment")
print(len(tracking))
if len(tracking)>0:
    data=data.merge(tracking,how='left',on='ship_id')
else:
    data['track']=None
    data['track_comment']=None
   
                    
anomalies=pd.read_parquet("../data/anomaly_detection_data")
#data=data.merge(anomalies,on='ship_id').sort_values("anomaly")

cols = list(data.columns)
cols = [cols[-2]] + [cols[-1]] + [cols[0]] + [cols[-4]] + [cols[-3]] + cols[1:-4]
data = data[cols]
data['id'] = data['ship_id']


app = Dash(external_stylesheets=[dbc.themes.DARKLY])
server = app.server
app.title = "AQUA"


max_rows=25



def get_polygon(lons, lats, color='blue'):
    if len(lons) != len(lats):
        raise ValueError('the legth of longitude list  must coincide with that of latitude')
    geojd = {"type": "FeatureCollection"}
    geojd['features'] = []
    coords = []
    for lon, lat in zip(lons, lats): 
        coords.append((lon, lat))   
    coords.append((lons[0], lats[0]))  #close the polygon  
    geojd['features'].append({ "type": "Feature",
                               "geometry": {"type": "Polygon",
                                            "coordinates": [coords] }})
                               
    layer=dict(sourcetype = 'geojson',
             source =geojd,
             below='',  
             type = 'line',
             color=color)
    return layer




load_figure_template("darkly")

app.layout = html.Div(children=[
    dbc.Row(dbc.Col([html.H1(children='AQUA')])),
    dbc.Row([dbc.Col([html.Div(children="Work in Progress - Feedback appreciated. Click on a row in the table right to visualize the ship")],width=6),
            dbc.Col([dcc.Input(id='filter-query-input', placeholder='Enter advanced filter query')],width=3),
            dbc.Col([html.Button('Save', id='save_button', n_clicks=0)],width=3)]),
    dbc.Row([
    dbc.Col([
        dcc.Graph(
            id='mapgraph',
            figure={}
            
       ),html.Div(id="blur-hidden-div")],id='showme',style={'display': 'block'},width='auto' ),
    dbc.Col([
            dash_table.DataTable(data.to_dict('records'),[{"name": i, "id": i} for i in data.columns], id='tbl',style_filter={'backgroundColor': 'black'}, 
style_header={'backgroundColor': 'black'},
style_cell={'backgroundColor': 'black', 'color': 'white'},style_data_conditional=[
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(45, 47, 51)',
        }
    ],style_table={
        'overflowX': 'auto'
    },page_size=max_rows,filter_action="native",
sort_action="native",sort_mode="multi",is_focused=False,editable=True,export_format='csv')
    ],width=6 )]),
    dcc.ConfirmDialog(
        id='output_generated',
        message='Text',
    )
])
#@callback(Output('tbl_out', 'children'), Input('tbl', 'active_cell'))
#def update_graphs(active_cell):
#    return str(active_cell) if active_cell else "Click the table"

@callback(Output('mapgraph', 'figure'),Output('showme', 'style'),Output('tbl','data'),
          State('tbl', 'data'), State('tbl', 'columns'),State('tbl','page_current'),Input('tbl','active_cell'))
              #Input('submit-button-state', 'n_clicks'),
              #State('ship_id', 'value'))
def update_chart(data_loc,columns_loc,page_current,active_cell):
    print(active_cell)
    if active_cell is None:
        raise PreventUpdate
    #if active_cell['column_id']!="ship_id":
    #    raise PreventUpdate
    #else:
    if page_current is None:
        page_current=0
    
    if "row_id" in active_cell:
        ship_id=active_cell['row_id']
        real_row=active_cell['row_id']
    else:
        real_row=active_cell['row']+page_current*max_rows
        ship_id=pd.DataFrame(data).iat[real_row,1]
    
    if active_cell['column_id']=='track' or active_cell['column_id']=='track_comment':
        
        row_number = data.index.get_loc(data[data['ship_id'] == ship_id].index[0])
        print(data_loc[row_number]['track'])

        
    
    print(ship_id)
    
    df=pd.read_parquet("../data/joined_data",
                    #filters=[('type', '>=', 80),('type', '<', 90)]
                    filters=[('ship_id', '==', int(ship_id))]
                    )
    #df=df.merge(anomalies,on='ship_id')

    #print(df.columns)

    fig = px.scatter_mapbox(df, 
                            lat="latitude_mean", 
                            lon="longitude_mean", 
                            hover_name="name", 
                            hover_data=['year', 'month', 'day', 'hour','ship_id_count', 'ship_id_count_static', 'sog_mean',
'cog_median', 'true_heading_isnot511_median', 
       'true_heading_is511_count', 'latitude_mean', 'latitude_min',
       'latitude_max', 'latitude_std', 'longitude_mean', 'longitude_min',
       'longitude_max', 'longitude_std', 'nav_status_mode1',
       'nav_status_mode2', 'nav_status_mode3', 'com_state_mode1',
       'com_state_mode2', 'com_state_mode3', 'imo_number', 'callsign',
       'type', 'fix_type', 'dim_a', 'dim_b', 'dim_c', 'dim_d',
       'eta_month_mode', 'eta_day_mode', 'eta_hour_mode', 'max_static_draught',
       'destination_mode','ship_id'],
                            color="day",
                            color_continuous_scale=px.colors.sequential.Rainbow,
                            #size="ColName",-
                            zoom=2, 
                            height=800,
                            width=800)

    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    mylayers =[]
    mylayers.append(get_polygon(lons=[-32, 109, 109, -32], lats=[83, 83, 30, 30],  color='black')) #83, -32], [30, 109
    fig.layout.update(mapbox_layers =mylayers)
    return fig,{'display': 'block'},data_loc



@callback(
    Output('tbl', 'filter_query'),
    Input('filter-query-input', 'value')
)
def write_query(query):
    if query is None:
        return ''
    return query
    
    
@callback(
    Output('save_button', 'children'),
    State('tbl','data'),
    Input('save_button', 'n_clicks'),
    prevent_initial_call=True
)
def update_output(data,n_clicks):
    print("UPDATE_OUTPUT")
    save_data=[]
    for i in range(len(data)):
        if data[i]['track'] is not None or data[i]['track_comment'] is not None:
            save_data.append({"track":data[i]['track'],"track_comment":data[i]['track_comment'],"ship_id":data[i]['ship_id']})
    
    df_save=pd.DataFrame(save_data)#,columns=["track","track_comment","ship_id"])
    table = pa.Table.from_pandas(df_save)
    os.system(f"rm -rf ../data/track_comment/*")    
    pq.write_to_dataset(table,root_path='../data/track_comment')


    now = datetime.now()
    
    return f'save (saved {now})'
    
    
    
    

app.run_server(debug=True)  # Turn off reloader if inside Jupyter