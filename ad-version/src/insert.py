# ==================================================================================
#  Copyright (c) 2020 HCL Technologies Limited.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ==================================================================================

"""
This Module is temporary for pushing data into influxdb before dpeloyment of AD xApp. It will depreciated in future, when data will be coming through KPIMON
"""

import datetime
import time
import pandas as pd
from influxdb_client import InfluxDBClient, Point
from configparser import ConfigParser

class INSERTDATA:
    def __init__(self):
        self.config()
        self.connect()

    def config(self):
        cfg = ConfigParser()
        cfg.read('ad_config.ini')
        if cfg.has_section('influxdb'):
            self.url = cfg.get('influxdb', "url")
            self.token = cfg.get('influxdb', "token")
            self.org = cfg.get('influxdb', "org")
            self.bucket = cfg.get('influxdb', "bucket")
            self.meas = cfg.get('influxdb', "measurement")

    def connect(self):
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.write_api = self.client.write_api()
        self.query_api = self.client.query_api()
        print("Connected to InfluxDB at {}".format(self.url))

    def dropmeas(self, measname):
        query = f'from(bucket: "{self.bucket}") |> range(start: -1y) |> filter(fn: (r) => r._measurement == "{measname}") |> drop()'
        self.query_api.query(query)
        print("Dropped measurement: " + measname)

    def assign_timestamp(self, df):
        df = df.copy()
        steps = df['measTimeStampRf'].unique()
        for timestamp in steps:
            d = df[df['measTimeStampRf'] == timestamp]
            d.index = pd.date_range(start=datetime.datetime.now(), freq='1ms', periods=len(d))
            points = [
                Point(self.meas)
                .tag("tag_key", "tag_value")
                .field(col, row[col])
                .time(row.name)
                for _, row in d.iterrows()
                for col in d.columns
            ]
            self.write_api.write(bucket=self.bucket, record=points)
            time.sleep(0.7)

def populatedb():
    # Initiate connection and process data
    db = INSERTDATA()
    df = pd.read_csv('ue.csv')
    while True:
        db.assign_timestamp(df)

if __name__ == "__main__":
    populatedb()



# import datetime
# import time
# import pandas as pd
# from database import DATABASE
# from configparser import ConfigParser


# class INSERTDATA(DATABASE):

#     def __init__(self):
#         super().__init__()
#         self.config()
#         self.connect()
#         self.dropdb('RIC-Test')
#         self.createdb('RIC-Test')

#     def config(self):
#         cfg = ConfigParser()
#         cfg.read('ad_config.ini')
#         for section in cfg.sections():
#             if section == 'influxdb':
#                 self.host = cfg.get(section, "host")
#                 self.port = cfg.get(section, "port")
#                 self.user = cfg.get(section, "user")
#                 self.password = cfg.get(section, "password")
#                 self.path = cfg.get(section, "path")
#                 self.ssl = cfg.get(section, "ssl")
#                 self.dbname = cfg.get(section, "database")
#                 self.meas = cfg.get(section, "measurement")

#     def createdb(self, dbname):
#         if dbname not in self.client.get_list_database():
#             print("Create database: " + dbname)
#             self.client.create_database(dbname)
#             self.client.switch_database(dbname)

#     def dropdb(self, dbname):
#         if next((item for item in self.client.get_list_database() if item.get("name") == dbname), None) is not None:
#             print("DROP database: " + dbname)
#             self.client.drop_database(dbname)

#     def dropmeas(self, measname):
#         print("DROP MEASUREMENT: " + measname)
#         self.client.query('DROP MEASUREMENT '+measname)

#     def assign_timestamp(self, df):
#         steps = df['measTimeStampRf'].unique()
#         for timestamp in steps:
#             d = df[df['measTimeStampRf'] == timestamp]
#             d.index = pd.date_range(start=datetime.datetime.now(), freq='1ms', periods=len(d))
#             self.client.write_points(d, self.meas)
#             time.sleep(0.7)


# def populatedb():
#     # inintiate connection and create database UEDATA
#     db = INSERTDATA()
#     df = pd.read_csv('ue.csv')
#     while True:
#         db.assign_timestamp(df)


# if __name__ == "__main__":
#     populatedb()
