#from datetime import datetime, timezone
import datetime
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging
from logging.handlers import RotatingFileHandler
import sys
import re
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder


formatter = logging.Formatter("[%(asctime)s] [%(pathname)s:%(lineno)d] %(levelname)s - %(message)s")
handler = RotatingFileHandler('/app/log/06_Anomaly_Detection.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")



def anomaly_detection():

    pd.set_option('display.max_columns', None)
    fields_ad = [('ship_id', pa.int64()),('anomaly', pa.float64())]
    schema_ad = pa.schema(fields_ad)
    os.system(f"rm -rf data/anomaly_detection_data/*")
    df=pd.read_parquet("data/grouped_data")
    ship_id_mem=df['ship_id']
    df=df.drop(['ship_id','longitude_std','latitude_std','last_contact'],axis=1)
    
    #features_to_encode = ['type', 'fix_type']
    features_to_encode = []
    
    #new logic
    df=df.filter(['total_hours_avail','max_time_gap','destination_count',
                  'true_heading_is511_count'])

        
    scaler = StandardScaler()

    for column in df.columns:
        if column in features_to_encode:
            continue
        df[column] = scaler.fit_transform(df[column].values.reshape(-1,1))
        
    #ohe = OneHotEncoder()
    #data= ohe.fit_transform(df[features_to_encode])
    #df1 = pd.DataFrame(data.toarray(), columns=ohe.get_feature_names_out(), dtype=int)
    #df=pd.concat([df,df1],axis=1)
    
    
    model = IsolationForest(n_estimators=100, contamination=0.1)
    model.fit(df) 
    
    #y_pred = model.predict(df)
    df['anomaly'] = model.decision_function(df)
    df['anomaly'] = df['anomaly'].round(3)
    #df['anomaly'] = [True if x == -1 else False for x in y_pred]
    df['ship_id'] = ship_id_mem
    #print("ANOMALIES:"+str(df['anomaly'].sum()))
    df=df[df.anomaly<0]
    df_out=df[['ship_id','anomaly']]
    
    list_features=list(df.columns)
    list_features.remove("ship_id")
    list_features.remove("anomaly")
    
    
    
    for feature in list_features:
        model = IsolationForest(n_estimators=100, contamination=0.1)
        model.fit(df[[feature]]) 
        df['a_'+feature] = model.decision_function(df[[feature]])
        
    selected_columns = [column for column in df.columns if column.startswith('a_')]
    df_out['anomaly_prob_reason']=df.loc[:,selected_columns].idxmin(axis=1).copy()
    
    
    
    table = pa.Table.from_pandas(df_out)
        
    pq.write_to_dataset(table,root_path='data/anomaly_detection_data')

        
    
    
    
     
    

        


if __name__ == "__main__":

    anomaly_detection()
