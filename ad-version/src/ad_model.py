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

import joblib
from mdclogpy import Logger
from influxdb_client.client.query_api import QueryApi
import pandas as pd 
import numpy as np


logger = Logger(name=__name__)


class modelling(object):
    r""" Filter dataframe based on paramters that were used to train model
    use transormer to transform the data
    load model and predict the label(normal/anomalous)

    Parameters:
    data:DataFrame
    """

    def __init__(self, data=None):
        self.data = data
        self.load_model()
        self.load_param()
        self.load_scale()


    def load_model(self):
        try:
            with open('src/model', 'rb') as f:
                self.model = joblib.load(f)
        except FileNotFoundError:
            logger.error("Model Does not exsist")

    def load_param(self):
        try:
            with open('src/num_params', 'rb') as f:
                self.num = joblib.load(f)

        except FileNotFoundError:
            logger.error("Parameter file does not exsist")

    def load_scale(self):
        try:
            with open('src/scale', 'rb') as f:
                self.scale = joblib.load(f)
        except FileNotFoundError:
            logger.error("Scale file does not exsist")

    def transformation(self):
        self.data = self.scale.transform(self.data)

    def predict(self, df):
        """ Load the saved model and return predicted result.
        Parameters
        .........
        name:str
            name of model

        Return
        ......
        pred:int
            predict label on a given sample

        """
        self.data = df.loc[:, self.num]
        self.transformation()
        pred = self.model.predict(self.data)
        pred = [1 if p == -1 else 0 for p in pred]
        return pred


class CAUSE(object):
    r""""Rule basd method to find degradation type of anomalous sample

    Attributes:
    normal:DataFrame
        Dataframe that contains only normal sample
    """

    def __init__(self):
        self.normal = None

    def cause(self, df, db, threshold):
        """ Filter normal data for a particular ue-id to compare with a given sample
            Compare with normal data to find and return degradaton type
        """
        sample = df.copy()
        sample.index = range(len(sample))
        for i in range(min(len(sample), 10)):
            if sample.iloc[i]['Anomaly'] == 1:
                db_ue = 'tag_key'  # Set this to the correct column name if needed
                print("meas", db.meas)
                print("db_ue", db_ue)
                print("sample", sample)
                print("sample.iloc[i]", sample.iloc[i])
                print("sample.iloc[i][db_ue]", sample.iloc[i][db_ue])
                print("db", db)

                #default 20s
                query = f"""
                from(bucket: "{db.bucket}")
                |> range(start: -240h)
                |> filter(fn: (r) => r["_measurement"] == "{db.meas}" and r["{db_ue}"] == "{sample.iloc[i][db_ue]}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                """ 

                print(query)  # Print the query to verify its correctness

                normal = db.query(query)

                print("print normal", normal)
                print(normal.columns)

                if normal is not None and not normal.empty:
                    # Extract relevant columns from the 'normal' DataFrame
                    print("db.meas", db.meas)
                    normal = normal[[db.thpt, db.rsrp, db.rsrq]]
                    normal = normal.dropna()

                    print("normal 2", normal)
                    # Find the degradation using the 'find' method
                    print("threshold", threshold) 

                    deg = self.find(sample.loc[i, :], normal.max(), db, threshold)

                    if deg:
                        # Set 'Degradation' value in the sample DataFrame
                        sample.loc[i, 'Degradation'] = deg

                        # Determine the 'Anomaly' type based on the degradation information
                        if 'Throughput' in deg and ('RSRP' in deg or 'RSRQ' in deg):
                            sample.loc[i, 'Anomaly'] = 2  # Both types of anomalies detected
                        else:
                            sample.loc[i, 'Anomaly'] = 1  # Only one type of anomaly detected
                    else:
                        # No degradation found
                        sample.loc[i, 'Anomaly'] = 0

                else:
                    # Handle cases where 'normal' is None or empty
                    sample.loc[i, 'Anomaly'] = 0

        print("SAMPLE: ", sample)
        return sample[['Anomaly', 'Degradation']].values.tolist()

    def find(self, row, l, db, threshold):
        """ Store if a particular parameter is below threshold and return """
        deg = []
        print("row", row)
        print("thtpt", db.thpt)
        print("rsrp", db.rsrp)
        print("threshold", threshold)
        
        # Ensure row and l are Series and we are comparing individual elements
        thpt_val = row[db.thpt].iloc[0] if isinstance(row[db.thpt], pd.Series) else row[db.thpt]
        l_thpt_val = l[db.thpt].iloc[0] if isinstance(l[db.thpt], pd.Series) else l[db.thpt]
        if thpt_val < l_thpt_val * (100 - threshold) * 0.01:
            deg.append('Throughput')

        rsrp_val = row[db.rsrp].iloc[0] if isinstance(row[db.rsrp], pd.Series) else row[db.rsrp]
        l_rsrp_val = l[db.rsrp].iloc[0] if isinstance(l[db.rsrp], pd.Series) else l[db.rsrp]
        if rsrp_val < l_rsrp_val - 15:
            deg.append('RSRP')

        rsrq_val = row[db.rsrq].iloc[0] if isinstance(row[db.rsrq], pd.Series) else row[db.rsrq]
        l_rsrq_val = l[db.rsrq].iloc[0] if isinstance(l[db.rsrq], pd.Series) else l[db.rsrq]
        if rsrq_val < l_rsrq_val - 10:
            deg.append('RSRQ')
        
        if len(deg) == 0:
            deg = False
        else:
            deg = ' '.join(deg)
        return deg


