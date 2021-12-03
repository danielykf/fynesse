from typing import Optional

from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import mysql.connector as sql
import urllib.request as request
from .config import *

import pandas as pd

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

class Database:
  
  def __init__(self):
    self.connection: MySQLConnection = None
    self.cursor: MySQLCursor = None
    self.current_db: str = None

  def __getattr__(self, attr):
     return getattr(self.cursor, attr)
  
  def connect(self, url, username, password, **kwargs):
    try:
      self.connection = sql.connect(host=url, user=username, passwd=password, **kwargs)
      self.cursor = self.connection.cursor()
    except:
      print("Connection Error")
  
  def reconnect(self):
    self.connection.reconnect() 
  
  def use(self, db: str):
    self.cursor.execute(f"USE {db}")
    self.current_db = db


class Table:
    
  def __init__(self, database: Database, name: Optional[str]):
    self.database = database
    self.name = name

  def _load_csv (self, file_name, enclosed_by=''):
    self.database.execute(
      f"""
        LOAD DATA LOCAL INFILE '{file_name}' INTO TABLE `{self.name}`
        FIELDS TERMINATED BY ',' ENCLOSED BY '{enclosed_by}'
        LINES STARTING BY '' TERMINATED BY '\\n';
      """
    )
    
  @property
  def columns(self):
    self.database.execute(f"DESCRIBE {self.name}")
    column_details = self.database.fetchall()
    return list(map(lambda row: row[0], column_details))
  
  @property
  def exists(self):
    self.database.execute(f"SHOW TABLES LIKE '{self.name}'")
    results = self.database.fetchall()
    return results and self.name in results[0]
  
  def setup(self):
    raise NotImplementedError
  
  def to_df(self, limit = None):
    if limit:
      self.database.execute(f"SELECT * FROM {self.name} LIMIT {limit}")
    else:
      self.database.execute(f"SELECT * FROM {self.name}")

    data = self.database.fetchall()
    return pd.DataFrame(data, columns=self.columns)
  
  def view(self, limit=5):
    return self.to_df(limit)


class PropertiesTable(Table):
  
  def __init__(self, database: Database, name = "pp_data"):
    super().__init__(database, name)
  
  def setup(self):
    if self.exists:
      print(f"Table `{self.name}` already exists in database.")
      return
    
    self._create_table_if_not_exists()
    self._load_data()
    self._create_index()
  
  def _create_table_if_not_exists(self):
    self.database.execute(
      f"""
        CREATE TABLE IF NOT EXISTS `{self.name}` (
          `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
          `street` tinytext COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
          `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
      """
    )
  
  def _load_data(self):
    csv_url_template = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-[year]-part[part].csv"
    for year in range(2020, 1994, -1):
      for part in range(1, 3):
        csv_url = csv_url_template.replace("[year]", str(year)).replace("[part]", str(part))
        path = f"./data/pp-{year}-{part}.csv"
        
        request.urlretrieve(csv_url, path)
        self._load_csv(path, '"')
        print(path)
  
  def _create_index(self):
    iterator = self.database.execute(
      f"""
        ALTER TABLE `{self.name}`
        ADD PRIMARY KEY (`db_id`),
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
        CREATE INDEX `pp.postcode` USING HASH
          ON `{self.name}`
            (postcode);
        CREATE INDEX `pp.date` USING HASH
          ON `{self.name}` 
            (date_of_transfer);
      """,
      multi=True
    )

    for _ in iterator:
      pass
    

class PostcodeTable(Table):
  
  def __init__(self, database: Database, name = "postcode_data"):
    super().__init__(database, name)
  
  def setup(self):
    if self.exists:
      print(f"Table `{self.name}` already exists in database.")
      return
    
    self._create_table_if_not_exists()
    self._load_data()
    self._create_index()
  
  def _create_table_if_not_exists(self):
    self.database.execute(
      f"""
        CREATE TABLE IF NOT EXISTS `{self.name}` (
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `status` enum('live','terminated') NOT NULL,
          `usertype` enum('small', 'large') NOT NULL,
          `easting` int unsigned,
          `northing` int unsigned,
          `positional_quality_indicator` int NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
          `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
          `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
          `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
          `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
          `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
          `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
      """
    )
  
  def _load_data(self):
    self._load_csv("open_postcode_geo.csv")
    
  def _create_index(self):
    iterator = self.database.execute(
      f"""
        ALTER TABLE `{self.name}`
        ADD PRIMARY KEY (`db_id`),
        MODIFY `db_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1;
        CREATE INDEX `po.postcode` USING HASH
          ON `postcode_data`
            (postcode);
        CREATE INDEX `po.latitude` USING HASH
          ON `postcode_data`
            (latitude);
      """,
      multi=True
    )

    for _ in iterator:
      pass


class PricesCoordinatesTable(Table):
  
  def __init__(self, database: Database, name = "prices_coordinates_data"):
    super().__init__(database, name)
  
  def setup(self):
    if self.exists:
      print(f"Table `{self.name}` already exists in database.")
      return
    
    self._create_table_if_not_exists()
  
  def _create_table_if_not_exists(self):
    self.database.execute(
      """
        CREATE TABLE IF NOT EXISTS `prices_coordinates_data` (
          `price` int(10) unsigned NOT NULL,
          `date_of_transfer` date NOT NULL,
          `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
          `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
          `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
          `locality` tinytext COLLATE utf8_bin NOT NULL,
          `town_city` tinytext COLLATE utf8_bin NOT NULL,
          `district` tinytext COLLATE utf8_bin NOT NULL,
          `county` tinytext COLLATE utf8_bin NOT NULL,
          `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
          `latitude` decimal(11,8) NOT NULL,
          `longitude` decimal(10,8) NOT NULL,
          `db_id` bigint(20) unsigned NOT NULL
        ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
      """
    )
