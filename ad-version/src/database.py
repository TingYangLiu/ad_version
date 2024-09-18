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

import time
import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import WriteApi
from configparser import ConfigParser
from mdclogpy import Logger
from requests.exceptions import RequestException, ConnectionError

logger = Logger(name=__name__)

class DATABASE(object):
    """DATABASE class to handle InfluxDB v2.x connections and operations.

    Parameters
    ----------
    url: str (default='http://localhost:8086')
        URL to connect to InfluxDB
    token: str
        Token for authentication
    org: str
        Organization name in InfluxDB
    bucket: str
        Bucket name in InfluxDB

    Attributes
    ----------
    client: InfluxDBClient
        InfluxDB client instance
    query_api: QueryApi
        Query API instance
    write_api: WriteApi
        Write API instance
    data: DataFrame
        Fetched data from the database
    """

    def __init__(self, url='http://10.109.234.168:80', token='qhIq6TFXk0ndJrHQAlzrjvWXq9laFBGu', org='influxdata', bucket='RIC-Test'):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.data = None
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self.query_api = self.client.query_api()
        self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=5000))
        self.config()
        self.connect()

    def connect(self):
        if self.client:
            self.client.close()
        
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.query_api = self.client.query_api()
            self.write_api = self.client.write_api(write_options=WriteOptions(batch_size=5000))
            logger.info(f"Connected to InfluxDB at {self.url}")
            return True
        except (RequestException, ConnectionError) as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            time.sleep(120)
            return False

    def read_data(self, train=False, valid=False, limit=False):
        """Read data method for a given measurement and limit using Flux query language.

        Parameters
        ----------
        meas: str (default=None)
            Measurement name
        train: bool (default=False)
            If True, retrieves training data within a specific time range
        valid: bool (default=False)
            If True, retrieves validation data from the last 5 minutes
        limit: int or bool (default=False)
            If an integer, limits the number of records retrieved
        """
        self.data = None

        if self.meas is None:
            logger.error("Measurement name must be provided")
            return

        # Construct the Flux query
        # Construct the Flux query
        query = (
            f'from(bucket: "{self.bucket}") '
            f'|> range(start: -240h) '
            f'|> filter(fn: (r) => r._measurement == "{self.meas}") '
        )


        # Apply filters based on parameters
        if train:
            query += '|> range(start: -240h, stop: -5m) '
        elif valid:
            query += '|> range(start: -240h) '
        if limit:
            query += f'|> limit(n: {limit}) '

        query += '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
        
        result = self.query_api.query_data_frame(query)
        print("self meas is : ", self.meas)
        print("value of result is database.py : ", result)

        self.data = result  # Use double brackets to select the column as DataFrame


    def write_anomaly(self, df, meas='AD'):
        """Write anomaly data to InfluxDB

        Parameters
        ----------
        df: DataFrame
            Data to be written
        meas: str
            Measurement name
        """
        try:
            points = []
            for _, row in df.iterrows():
                # Convert the timestamp to a string format
                timestamp = row["measTimeStampRf"].strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(row["measTimeStampRf"], pd.Timestamp) else row["measTimeStampRf"]
                
                # Create a Point for each row, adding tags and fields
                point = Point(meas).tag("tag_key", "tag_value").time(timestamp)
                for col in df.columns:
                    if col != "measTimeStampRf":  # Skip the timestamp column
                        point.field(col, row[col])
                
                points.append(point)

            # Write points to InfluxDB
            # Error Write Anomaly Data to InfluxDB
            # self.write_api.write(bucket=self.bucket, org=self.org , record=points)
        except (RequestException, ConnectionError) as e:
            logger.error(f"Failed to write data to InfluxDB: {e}")

    def query(self, query):
        try:
            # Execute the query
            result = self.query_api.query_data_frame(query)
            
            # Ensure the result is not empty or malformed
            if result is not None and not result.empty:
                return result
            else:
                logger.warning("Query returned no results or empty data.")
                return pd.DataFrame()
        except (RequestException, ConnectionError) as e:
            logger.error(f"Failed to execute query due to a request error: {e}")
            return pd.DataFrame()  # Return an empty DataFrame for consistency
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return pd.DataFrame()  # Return an empty DataFrame for consistency


    def config(self):
        cfg = ConfigParser()
        cfg.read('src/ad_config.ini')
        if cfg.has_section('influxdb'):
            self.url = cfg.get('influxdb', 'url')
            self.token = cfg.get('influxdb', 'token')
            self.org = cfg.get('influxdb', 'org')
            self.bucket = cfg.get('influxdb', 'bucket')
            self.meas = cfg.get('influxdb', "measurement")

        if cfg.has_section('features'):
            self.thpt = cfg.get('features', "thpt")
            self.rsrp = cfg.get('features', "rsrp")
            self.rsrq = cfg.get('features', "rsrq")
            self.rssinr = cfg.get('features', "rssinr")
            self.prb = cfg.get('features', "prb_usage")
            self.ue = cfg.get('features', "ue")
            self.anomaly = cfg.get('features', "anomaly")
            self.a1_param = cfg.get('features', "a1_param")

class DUMMY(DATABASE):
    def __init__(self):
        super().__init__()
        self.ue_data = pd.read_csv('src/ue.csv')

    def connect(self):
        return True

    def read_data(self, train=False, valid=False, limit=100000):
        if not train:
            self.data = self.ue_data.head(limit)
        else:
            self.data = self.ue_data.head(limit).drop(columns=['anomaly'], errors='ignore')

    def write_anomaly(self, df, meas_name='AD'):
        pass

    def query(self, query=None):
        return {'UEReports': self.ue_data.head(1)}



# import time
# import pandas as pd
# from influxdb import DataFrameClient
# from configparser import ConfigParser
# from mdclogpy import Logger
# from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
# from requests.exceptions import RequestException, ConnectionError

# logger = Logger(name=__name__)


# class DATABASE(object):
#     r""" DATABASE takes an input as database name. It creates a client connection
#       to influxDB and It reads/ writes UE data for a given dabtabase and a measurement.


#     Parameters
#     ----------
#     host: str (default='r4-influxdb.ricplt.svc.cluster.local')
#         hostname to connect to InfluxDB
#     port: int (default='8086')
#         port to connect to InfluxDB
#     username: str (default='root')
#         user to connect
#     password: str (default='root')
#         password of the use

#     Attributes
#     ----------
#     client: influxDB client
#         DataFrameClient api to connect influxDB
#     data: DataFrame
#         fetched data from database
#     """

#     def __init__(self, dbname='Timeseries', user='root', password='root', host="r4-influxdb.ricplt", port='8086', path='', ssl=False):
#         self.data = None
#         self.host = host
#         self.port = port
#         self.user = user
#         self.password = password
#         self.path = path
#         self.ssl = ssl
#         self.dbname = dbname
#         self.client = None
#         self.config()

#     def connect(self):
#         if self.client is not None:
#             self.client.close()

#         try:
#             self.client = DataFrameClient(self.host, port=self.port, username=self.user, password=self.password, path=self.path, ssl=self.ssl, database=self.dbname, verify_ssl=self.ssl)
#             version = self.client.request('ping', expected_response_code=204).headers['X-Influxdb-Version']
#             logger.info("Conected to Influx Database, InfluxDB version : {}".format(version))
#             return True

#         except (RequestException, InfluxDBClientError, InfluxDBServerError, ConnectionError):
#             logger.error("Failed to establish a new connection with InflulxDB, Please check your url/hostname")
#             time.sleep(120)

#     def read_data(self, train=False, valid=False, limit=False):
#         """Read data method for a given measurement and limit

#         Parameters
#         ----------
#         meas: str (default='ueMeasReport')
#         limit:int (defualt=False)
#         """
#         self.data = None
#         query = 'select * from ' + self.meas
#         if not train and not valid and not limit:
#             query += ' where time>now()-1600ms'
#         elif train:
#             query += ' where time<now()-5m and time>now()-75m'
#         elif valid:
#             query += ' where time>now()-5m'
#         elif limit:
#             query += ' where time>now()-1m limit '+str(limit)
#         result = self.query(query)
#         if result and len(result[self.meas]) != 0:
#             self.data = result[self.meas]

#     def write_anomaly(self, df, meas='AD'):
#         """Write data method for a given measurement

#         Parameters
#         ----------
#         meas: str (default='AD')
#         """
#         try:
#             self.client.write_points(df, meas)
#         except (RequestException, InfluxDBClientError, InfluxDBServerError) as e:
#             logger.error('Failed to send metrics to influxdb')
#             print(e)

#     def query(self, query):
#         try:
#             result = self.client.query(query)
#         except (RequestException, InfluxDBClientError, InfluxDBServerError, ConnectionError) as e:
#             logger.error('Failed to connect to influxdb: {}'.format(e))
#             result = False
#         return result

#     def config(self):
#         cfg = ConfigParser()
#         cfg.read('src/ad_config.ini')
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

#             if section == 'features':
#                 self.thpt = cfg.get(section, "thpt")
#                 self.rsrp = cfg.get(section, "rsrp")
#                 self.rsrq = cfg.get(section, "rsrq")
#                 self.rssinr = cfg.get(section, "rssinr")
#                 self.prb = cfg.get(section, "prb_usage")
#                 self.ue = cfg.get(section, "ue")
#                 self.anomaly = cfg.get(section, "anomaly")
#                 self.a1_param = cfg.get(section, "a1_param")


# class DUMMY(DATABASE):

#     def __init__(self):
#         super().__init__()
#         self.ue_data = pd.read_csv('src/ue.csv')

#     def connect(self):
#         return True

#     def read_data(self, train=False, valid=False, limit=100000):
#         if not train:
#             self.data = self.ue_data.head(limit)
#         else:
#             self.data = self.ue_data.head(limit).drop(self.anomaly, axis=1)

#     def write_anomaly(self, df, meas_name='AD'):
#         pass

#     def query(self, query=None):
#         return {'UEReports': self.ue_data.head(1)}
