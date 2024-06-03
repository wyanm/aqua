from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging
from logging.handlers import RotatingFileHandler
import re
import sys


formatter = logging.Formatter("[%(asctime)s] [%(pathname)s:%(lineno)d] %(levelname)s - %(message)s")
handler = RotatingFileHandler('log/02.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")



def agg_data():
    fields_pos_agg = [('ship_id', pa.int64()),('ship_id_count', pa.int64()),
              ('year', pa.int64()),('month', pa.int64()),('day', pa.int64()),('hour', pa.int64()),
              ('sog_mean', pa.float64()),('sog_min', pa.float64()),('sog_max', pa.float64()),
              ('cog_median', pa.float64()),('cog_min', pa.float64()),('cog_max', pa.float64()),
              ('true_heading_isnot511_median', pa.float64()),('true_heading_isnot511_min', pa.int64()),('true_heading_isnot511_max', pa.int64()),('true_heading_isnot511_std', pa.float64()),('true_heading_is511_count', pa.int64()),
              ('latitude_mean', pa.float64()),('latitude_min', pa.float64()),('latitude_max', pa.float64()),('latitude_std', pa.float64()),
              ('longitude_mean', pa.float64()),('longitude_min', pa.float64()),('longitude_max', pa.float64()),('longitude_std', pa.float64()),
                ('nav_status_mode1', pa.int64()),('nav_status_mode2', pa.int64()),('nav_status_mode3', pa.int64()),
                ('com_state_mode1', pa.int64()),('com_state_mode2', pa.int64()),('com_state_mode3', pa.int64())]
    schema_pos_agg = pa.schema(fields_pos_agg)
    now = datetime.now(timezone.utc)
    string_now = f"year={now.year}/month={now.month}/day={now.day}/hour={now.hour}"
    for x in os.walk("position_reports"):
        if "hour" not in x[0]:
            continue
        if string_now in x[0]:
            continue
        logger.info(x[0])
        df=pd.read_parquet(x[0])
        
     
        
        df_agg=df.groupby('ship_id').agg(
            ship_id=('ship_id','first'),
            ship_id_count=('ship_id','count'),
            #year=('year','first'),
            #month=('month','first'),
            #day=('day','first'),
            #hour=('hour','first'),
            sog_mean=('sog','mean'),
            sog_min=('sog','min'),
            sog_max=('sog','max'),
            sog_std=('sog','std'),
            cog_median=('cog','median'),
            cog_min=('cog','min'),
            cog_max=('cog','max'),
            true_heading_isnot511_median=('true_heading',lambda x:(x!=511).median()),
            true_heading_isnot511_min=('true_heading',lambda x:(x!=511).min()),
            true_heading_isnot511_max=('true_heading',lambda x:(x!=511).max()),
            true_heading_isnot511_std=('true_heading',lambda x:(x!=511).std()),
            true_heading_is511_count=('true_heading',lambda x:(x==511).count()),
            longitude_mean=('longitude','mean'),
            longitude_min=('longitude','min'),
            longitude_max=('longitude','max'),
            longitude_std=('longitude','std'),
            latitude_mean=('latitude','mean'),
            latitude_min=('latitude','min'),
            latitude_max=('latitude','max'),
            latitude_std=('latitude','std'),
            nav_status_mode1=('nav_status',lambda x:x.value_counts().index[0]),
            nav_status_mode2=('nav_status',lambda x:x.value_counts().index[min(1,len(x.value_counts())-1)]),
            nav_status_mode3=('nav_status',lambda x:x.value_counts().index[min(2,len(x.value_counts())-1)]),
            com_state_mode1=('com_state',lambda x:x.value_counts().index[0]),
            com_state_mode2=('com_state',lambda x:x.value_counts().index[min(1,len(x.value_counts())-1)]),
            com_state_mode3=('com_state',lambda x:x.value_counts().index[min(2,len(x.value_counts())-1)])         
        )
        
        df_agg['year'] = int(re.search('(?<=year=)(.+?)/', x[0])[0][:-1])
        df_agg['month'] = int(re.search('(?<=month=)(.+?)/', x[0])[0][:-1])
        df_agg['day'] = int(re.search('(?<=day=)(.+?)/', x[0])[0][:-1])
        df_agg['hour'] = int(re.search('(?<=hour=)(.+)', x[0])[0])
        
        
        table = pa.Table.from_pandas(df_agg, schema=schema_pos_agg)
        
        pq.write_to_dataset(table,root_path='agg_position_reports', schema=schema_pos_agg,partition_cols=['year','month','day','hour'])
        os.system(f"rm -rf {x[0]}")
        


if __name__ == "__main__":

    agg_data()
