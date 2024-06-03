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

formatter = logging.Formatter("[%(asctime)s] [%(pathname)s:%(lineno)d] %(levelname)s - %(message)s")
handler = RotatingFileHandler('/app/log/05_Group_By_Ship.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")



def group_data():
    fields_group = [('ship_id', pa.int64()),('total_hours_avail', pa.int64()),('static_count', pa.int64()),
                    ('ship_id_count', pa.int64()),('max_time_gap', pa.int64()),
                    ('black_sea', pa.int64()),('north_west_russia', pa.int64()),
                    ('type', pa.int64()),('fix_type', pa.int64()),('size', pa.int64()),('dim_a', pa.int64()),('max_static_draught', pa.float64()),
                    ('destination_count', pa.int64()),        
              ('sog_mean', pa.float64()),('sog_min', pa.float64()),('sog_max', pa.float64()),('true_heading_is511_count', pa.int64()),
              ('latitude_mean', pa.float64()),('latitude_min', pa.float64()),('latitude_max', pa.float64()),('latitude_std', pa.float64()),
              ('longitude_mean', pa.float64()),('longitude_min', pa.float64()),('longitude_max', pa.float64()),('longitude_std', pa.float64()),
              ('distance_sum', pa.float64()),('distance_simple', pa.float64()),('last_contact', pa.time64('ns'))]
        
    
    schema_group = pa.schema(fields_group)
    os.system(f"rm -rf data/grouped_data/*")
    df=pd.read_parquet("data/joined_data")# FILTER SET AT JOIN NOW,filters=[('type', '>=', 80),('type', '<', 90)])
    
    
        
    #df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['hour'])  # Combine days and hours into a datetime column
    #df['datetime'] = pd.to_timedelta(df['year'],unit="y") + pd.to_timedelta(df['month'],unit="months") + pd.to_timedelta(df['day'],unit="days") + pd.to_timedelta(df['hour'],unit="hours")
    #df['datetime'] = pd.to_datetime()
    df['datetime'] = df.apply(lambda row: datetime.datetime(row['year'], row['month'], row['day']) + datetime.timedelta(hours=row['hour']), axis=1)
    
    # Resample the data to include all hours for each product-id
    #df_resampled = df.set_index('datetime').groupby('ship_id').resample('H').asfreq()
    #df.reset_index(drop=True, inplace=True)
    #df.set_index('datetime', inplace=True)

    # Calculate the time difference between consecutive rows for each product-id
    df=df.sort_values('datetime')
    df['time_diff'] = df.groupby('ship_id')['datetime'].diff()
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    #print(df)

    # Find the maximum time difference for each product-id
    #max_time_diff = (df.groupby('ship_id')['time_diff'].max()/pd.Timedelta('1 hour')).fillna(-1)
    
    df['dim_sum']=df['dim_a']+df['dim_b']+df['dim_c']+df['dim_d']
    
    df['distance_sum'] = np.sqrt((df['longitude_max']-df['longitude_min']) ** 2 + (df['latitude_max']-df['latitude_min']) ** 2)
    
    df['black_sea']=(df['latitude_max']>40) & (df['latitude_min']<47.5) & (df['longitude_max']>26) & (df['longitude_min']<42)
    df['north_west_russia']=(df['latitude_max']>58) & (df['latitude_min']<=83) & (df['longitude_max']>10) & (df['longitude_min']<=109)
    
    df_agg=df.groupby(['ship_id']).agg(
            ship_id=('ship_id','first'),
            total_hours_avail=('ship_id','count'),#counting in how many hour-"units" the ship_id was present
            static_count=('ship_id_count_static','sum'),
            ship_id_count=('ship_id_count','sum'),
            max_time_gap=('time_diff',lambda x:x.max()/pd.Timedelta('1 hour')),
            last_contact=('datetime','max'),
            type=('type','first'),
            fix_type=('fix_type','first'),
            size=('dim_sum','first'),
            dim_a=('dim_a','first'),
            max_static_draught=('max_static_draught','first'),
            destination_count=('destination_mode',lambda x:len(x.value_counts())),
            sog_mean=('sog_mean','mean'),
            sog_min=('sog_min','mean'),
            sog_max=('sog_max','mean'),
            #sog_std=('sog_std','mean'),
            true_heading_is511_count=('true_heading_is511_count','sum'),
            longitude_mean=('longitude_mean','mean'),
            longitude_min=('longitude_min','min'),
            longitude_max=('longitude_max','max'),
            longitude_std=('longitude_std','mean'),
            latitude_mean=('latitude_mean','mean'),
            latitude_min=('latitude_min','min'),
            latitude_max=('latitude_max','max'),
            latitude_std=('latitude_std','mean'),
            distance_sum=('distance_sum','sum'),
            black_sea=('black_sea','sum'),
            north_west_russia=('north_west_russia','sum')
            )
    """
            nav_status_mode1=('nav_status',lambda x:x.value_counts().index[0]),
            nav_status_mode2=('nav_status',lambda x:x.value_counts().index[min(1,len(x.value_counts())-1)]),
            nav_status_mode3=('nav_status',lambda x:x.value_counts().index[min(2,len(x.value_counts())-1)]),
            com_state_mode1=('com_state',lambda x:x.value_counts().index[0]),
            com_state_mode2=('com_state',lambda x:x.value_counts().index[min(1,len(x.value_counts())-1)]),
            com_state_mode3=('com_state',lambda x:x.value_counts().index[min(2,len(x.value_counts())-1)])   
        ) """
    
    df_agg['max_time_gap']=df_agg['max_time_gap'].fillna(-1).astype(int)
    df_agg['distance_simple']=np.sqrt((df_agg['longitude_max']-df_agg['longitude_min']) ** 2 + (df_agg['latitude_max']-df_agg['latitude_min']) ** 2)

    
    #print(df_agg.head())


    df_agg=df_agg.round(3)
     

        
    table = pa.Table.from_pandas(df_agg, schema=schema_group)
        
    pq.write_to_dataset(table,root_path='data/grouped_data', schema=schema_group,partition_cols=['type'])

        


if __name__ == "__main__":

    group_data()
