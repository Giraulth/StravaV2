import folium
import polyline
import streamlit as st
from streamlit_folium import st_folium
from clickhouse_driver import Client
from dotenv import load_dotenv

import os
import geoloc

client = Client('localhost')

df = client.query_dataframe("SELECT name, polyline, kom, pr_elapsed_time, start_latlng, end_latlng FROM segment_final WHERE effort_count < 1000")

df_kom = df[df['pr_elapsed_time'] == df['kom']]

load_dotenv()

address = os.getenv('ADDRESS')

start_coords = geoloc.get_coordinates(address)
folium_map = folium.Map(location=start_coords, zoom_start=12)

for index, row in df.iterrows():
    coordinates = polyline.decode(row['polyline'])
    folium.PolyLine(locations=coordinates, color="green", weight=5, popup=row['name']).add_to(folium_map)

for index, row in df_kom.iterrows():
    coordinates = polyline.decode(row['polyline'])
    start_latlng = row['start_latlng']
    end_latlng = row['end_latlng']
    folium.PolyLine(locations=coordinates, color="yellow", weight=5, popup=row['name']).add_to(folium_map)

    folium.Marker(location=start_latlng, popup="Start: " + row['name'], icon=folium.Icon(color='green', icon='circle', prefix='fa')).add_to(folium_map)
    folium.Marker(location=end_latlng, popup="End: " + row['name'], icon=folium.Icon(color='red', icon='flag-checkered', prefix='fa')).add_to(folium_map)
    
st_folium(folium_map, width=700, height=500)
