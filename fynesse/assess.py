import datetime

import geopandas as gpd
import matplotlib.pyplot as plt
import mlai.plot as plot
import numpy as np
import osmnx as ox
import pandas as pd

from . import access
from .access import *
from .config import *

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""

class DataPipeline:
  
  def __init__(self, properties: PropertiesTable, postcode: PostcodeTable, prices_coordinates: PricesCoordinatesTable) -> None:
    self.properties = properties
    self.postcode = postcode
    self.prices_coordinates = prices_coordinates
    
    self.north = None
    self.south = None
    self.west = None
    self.east = None
    
    self.poi_radius = 1. # in km
    
  
  @property
  def bounding_box_exists(self):
    return all([self.north, self.south, self.west, self.east])
  
  
  def set_bounding_box(self, latitude, longitude, box_height, box_width):
    self.north = latitude + box_height / 2
    self.south = latitude - box_height / 2
    self.west = longitude - box_width / 2
    self.east = longitude + box_width / 2
  
  
  def _join_properties_and_postcode(self, poi_radius_deg, date, property_type):
    if not self.bounding_box_exists:
      raise ValueError("DataPipeline: The bounding box has not been set.")
    
    north = self.north + poi_radius_deg
    south = self.south - poi_radius_deg
    west = self.west - poi_radius_deg
    east = self.east + poi_radius_deg
    
    weeks = 26
    date_format = "%Y-%m-%d"
    start_date = datetime.datetime.strptime(date, date_format) - datetime.timedelta(weeks=weeks)
    end_date = datetime.datetime.strptime(date, date_format) + datetime.timedelta(weeks=weeks)
    start_date_str = start_date.strftime(date_format)
    end_date_str = end_date.strftime(date_format)
    
    self.prices_coordinates.database.execute(f"TRUNCATE TABLE `{self.prices_coordinates.name}`")
    self.prices_coordinates.database.execute(
      f"""
        INSERT INTO {self.prices_coordinates.name}
        SELECT 
          price, date_of_transfer, pp_data.postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county, country, latitude, longitude, pp_data.db_id
        FROM
          (SELECT * FROM `{self.postcode.name}` 
          WHERE latitude BETWEEN {south} AND {north} 
          AND longitude BETWEEN {west} AND {east}) 
          po_f
        INNER JOIN {self.properties.name}
        ON
          {self.properties.name}.postcode = po_f.postcode
        WHERE
          property_type = '{property_type}'
          AND date_of_transfer BETWEEN '{start_date_str}' AND '{end_date_str}'
      """
    )


  def get_prices_coordinates_df(self, date, property_type):
    poi_radius_deg = self.poi_radius / 111. # crude approximation of km in deg
    self._join_properties_and_postcode(poi_radius_deg, date, property_type)
    df = self.prices_coordinates.to_df()
    
    # De-duplication
    columns = list(df.columns)
    columns.remove("db_id")
    df.drop_duplicates(subset=columns, inplace=True)    
    return df
  
  
  def get_pois(self):
    if not self.bounding_box_exists:
      raise ValueError("DataPipeline: bounding_box has not been set.")
    
    tags = {
      "amenity": True,
      "buildings": True,
      "historic": True,
      "leisure": True,
      "shop": True,
      "tourism": True
    }
    pois = ox.geometries_from_bbox(self.north, self.south, self.east, self.west, tags)
    return pois
  
  
  def join_df_with_pois(self, df):
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    gdf.crs = "EPSG:4326" # geometry in (latitude, longitude)
    gdf = gdf.to_crs(epsg=3035) # project geometry to "metre"
    gdf.geometry = gdf.geometry.buffer(self.poi_radius * 1000.)  # For every property, draw a circle of radius 1km
    
    pois = self.get_pois()
    pois = pois.to_crs(epsg=3035)
    pois = pois[["geometry"]]
    
    """
    Perform a spatial join on the augmented prices_coordinates_gdf and the pois. Each record in joined_gdf represents a correspondance between the location of a property and a point-of-interest with distance <= 1km.
    """
    joined_gdf = gpd.tools.sjoin(gdf, pois, how="left")
    
    """
    Add a column, "poi_count", on the prices_coordinates dataframe, indicating the number of pois within distance <= 1km of each property.
    """
    index = np.array(joined_gdf.index)
    keys, counts = np.unique(index, return_counts=True)
    df["poi_count"] = 0
    df.loc[keys, "poi_count"] = counts
    
    return df
    
  
  def get_dataset(self, latitude, longitude, date, property_type):
    self.set_bounding_box(latitude, longitude, 0.1, 0.1)
    df = self.get_prices_coordinates_df(date, property_type)
    df = self.join_df_with_pois(df)
    df = df.sample(frac=1).reset_index(drop=True) # Shuffle the rows, to make sure the train/validate split is even
    
    df_x = df[["latitude", "longitude", "poi_count"]].copy()
    df_x.latitude = np.float32(df_x.latitude) - latitude
    df_x.longitude = np.float32(df_x.longitude) - longitude
    
    df_x["days"] = (pd.to_datetime(df["date_of_transfer"], format="%Y-%m-%d") - pd.to_datetime(date, format="%Y-%m-%d")).dt.days
    df_x["one"] = 1
    
    x = df_x.to_numpy()
    y = np.asarray(df.price)
    return x, y
  
  
  def plot (self, latitude, longitude, date, property_type, box_dim=0.1):
    self.set_bounding_box(latitude, longitude, box_dim, box_dim)
    prices_coordinates_df = self.get_prices_coordinates_df(date, property_type)
    
    prices_coordinates_gdf = gpd.GeoDataFrame(
      prices_coordinates_df,
      geometry=gpd.points_from_xy(prices_coordinates_df.longitude, prices_coordinates_df.latitude)
    )
    prices_coordinates_gdf.crs = "EPSG:4326"
    
    pois = self.get_pois()
    graph = ox.graph_from_bbox(self.north, self.south, self.east, self.west)
    nodes, edges = ox.graph_to_gdfs(graph)
    fig, ax = plt.subplots(figsize=plot.big_figsize)

    # Plot street edges
    edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    ax.set_xlim([self.west, self.east])
    ax.set_ylim([self.south, self.north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs 
    pois.plot(ax=ax, color="blue", alpha=0.7, markersize=10)
    prices_coordinates_gdf.plot(ax=ax, column="price", cmap="Reds", legend=True, markersize=10)
    plt.tight_layout()
