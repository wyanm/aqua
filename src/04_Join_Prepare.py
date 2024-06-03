from datetime import datetime, timezone
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
handler = RotatingFileHandler('/app/log/04_Join_Prepare.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")



def join_data():
    fields_join = [('ship_id', pa.int64()),('ship_id_count', pa.int64()),('ship_id_count_static', pa.int64()),
              ('year', pa.int64()),('month', pa.int64()),('day', pa.int64()),('hour', pa.int64()),
              ('sog_mean', pa.float64()),('sog_min', pa.float64()),('sog_max', pa.float64()),
              ('cog_median', pa.float64()),('cog_min', pa.float64()),('cog_max', pa.float64()),
              ('true_heading_isnot511_median', pa.float64()),('true_heading_isnot511_min', pa.int64()),('true_heading_isnot511_max', pa.int64()),('true_heading_isnot511_std', pa.float64()),('true_heading_is511_count', pa.int64()),
              ('latitude_mean', pa.float64()),('latitude_min', pa.float64()),('latitude_max', pa.float64()),('latitude_std', pa.float64()),
              ('longitude_mean', pa.float64()),('longitude_min', pa.float64()),('longitude_max', pa.float64()),('longitude_std', pa.float64()),
                ('nav_status_mode1', pa.int64()),('nav_status_mode2', pa.int64()),('nav_status_mode3', pa.int64()),
                ('com_state_mode1', pa.int64()),('com_state_mode2', pa.int64()),('com_state_mode3', pa.int64()),
                          ('imo_number', pa.int64()), ('callsign', pa.string()),
              ('name', pa.string()),('type', pa.int64()),('fix_type', pa.int64()),
              ('dim_a', pa.int64()),('dim_b', pa.int64()),('dim_c', pa.int64()),('dim_d', pa.int64()),
              ('eta_month_mode', pa.int64()),('eta_day_mode', pa.int64()),('eta_hour_mode', pa.int64()),
              ('max_static_draught', pa.float64()),('destination_mode', pa.string())]
    
    
    schema_join = pa.schema(fields_join)
    os.system(f"rm -rf data/joined_data/*")
    for x in os.walk("data/agg_position_reports"):
        if "hour" not in x[0]:
            continue
        logger.info(x[0])
        df=pd.read_parquet(x[0])
        y=x[0].replace("agg_position_reports","agg_static_ship_data")
        logger.info(y)
        if os.path.exists(y):
            
            df_ship=pd.read_parquet(y)
            df_ship=df_ship.rename(columns={'ship_id_count': 'ship_id_count_static'})        
            df=df.merge(df_ship,on='ship_id')
        else:
            logger.info("Ship Static Data not available for that hour")
            df['ship_id_count_static']=np.NaN
            df['imo_number']=np.NaN
            df['callsign']=pd.NA
            df['name']=pd.NA
            df['type']=np.NaN
            df['fix_type']=np.NaN
            df['dim_a']=np.NaN
            df['dim_b']=np.NaN
            df['dim_c']=np.NaN
            df['dim_d']=np.NaN
            df['eta_month_mode']=np.NaN
            df['eta_day_mode']=np.NaN
            df['eta_hour_mode']=np.NaN
            df['max_static_draught']=np.NaN
            df['destination_mode']=pd.NA

     
        df['year'] = int(re.search('(?<=year=)(.+?)/', x[0])[0][:-1])
        df['month'] = int(re.search('(?<=month=)(.+?)/', x[0])[0][:-1])
        df['day'] = int(re.search('(?<=day=)(.+?)/', x[0])[0][:-1])
        df['hour'] = int(re.search('(?<=hour=)(.+)', x[0])[0])
        
        # ACTIVATE FILTER OR NOT?
        df= df[(df['type'] >=80)&(df['type'] <90)]
        #df= df[(df['dim_a'] >=148)&(df['dim_a'] <178)]
        
        df=df.round(3)
        
        table = pa.Table.from_pandas(df, schema=schema_join)
        
        pq.write_to_dataset(table,root_path='data/joined_data', schema=schema_join,partition_cols=['year','month','day','hour'])

        


if __name__ == "__main__":

    join_data()
