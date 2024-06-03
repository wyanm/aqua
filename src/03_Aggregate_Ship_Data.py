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
handler = RotatingFileHandler('/app/log/03.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")



def agg_data():
    fields_static_agg = [('ship_id', pa.int64()),('ship_id_count', pa.int64()),
                          ('year', pa.int64()),('month', pa.int64()),('day', pa.int64()),('hour', pa.int64()),
                          ('imo_number', pa.int64()), ('callsign', pa.string()),
              ('name', pa.string()),('type', pa.int64()),('fix_type', pa.int64()),
              ('dim_a', pa.int64()),('dim_b', pa.int64()),('dim_c', pa.int64()),('dim_d', pa.int64()),
              ('eta_month_mode', pa.int64()),('eta_day_mode', pa.int64()),('eta_hour_mode', pa.int64()),
              ('max_static_draught', pa.float64()),('destination_mode', pa.string())]
    schema_static_agg = pa.schema(fields_static_agg)
    now = datetime.now(timezone.utc)
    string_now = f"year={now.year}/month={now.month}/day={now.day}/hour={now.hour}"
    for x in os.walk("data/static_ship_data"):
        if "hour" not in x[0]:
            continue
        if string_now in x[0]:
            continue
        logger.info(x[0])
        df=pd.read_parquet(x[0])
        #df['year']=pd.to_datetime(df['time'].astype(str)).dt.year
        #df['month']=pd.to_datetime(df['time'].astype(str)).dt.month
        #df['day']=pd.to_datetime(df['time'].astype(str)).dt.day
        #df['hour']=pd.to_datetime(df['time'].astype(str)).dt.hour
        #print(df.head())        
        df_agg=df.groupby(['ship_id']).agg(
            ship_id=('ship_id','first'),
            ship_id_count=('ship_id','count'),
            #year=('year','first'),
            #month=('month','first'),
            #day=('day','first'),
            #hour=('hour','first'),
            imo_number=('imo_number','first'),
            callsign=('callsign','first'),
            name=('name','first'),
            type=('type','first'),
            fix_type=('fix_type','first'),
            dim_a=('dim_a','first'),
            dim_b=('dim_b','first'),
            dim_c=('dim_c','first'),
            dim_d=('dim_d','first'),
            eta_month_mode=('eta_month',lambda x:x.value_counts().index[0]),
            eta_day_mode=('eta_day',lambda x:x.value_counts().index[0]),
            eta_hour_mode=('eta_hour',lambda x:x.value_counts().index[0]),
            max_static_draught=('max_static_draught','first'),
            destination_mode=('destination',lambda x:x.value_counts().index[0])     
        )
        df_agg['year'] = int(re.search('(?<=year=)(.+?)/', x[0])[0][:-1])
        df_agg['month'] = int(re.search('(?<=month=)(.+?)/', x[0])[0][:-1])
        df_agg['day'] = int(re.search('(?<=day=)(.+?)/', x[0])[0][:-1])
        df_agg['hour'] = int(re.search('(?<=hour=)(.+)', x[0])[0])
        
        table = pa.Table.from_pandas(df_agg, schema=schema_static_agg)
        
        pq.write_to_dataset(table,root_path='data/agg_static_ship_data', schema=schema_static_agg,partition_cols=['year','month','day','hour'])
        os.system(f"rm -rf {x[0]}")
        


if __name__ == "__main__":

    agg_data()
