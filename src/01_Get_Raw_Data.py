import asyncio
import websockets
import json
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from logging.handlers import RotatingFileHandler
import os
import sys


formatter = logging.Formatter("[%(asctime)s] [%(pathname)s:%(lineno)d] %(levelname)s - %(message)s")
handler = RotatingFileHandler('/app/log/01.log', maxBytes=1000000, backupCount=1)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger= logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("++++++STARTED++++++")




async def connect_ais_stream(pos_data,static_data,schema,schema_static):
    chunksize_pos = 1e4
    chunksize_static = 1e4
    start_time_pos = datetime.now(timezone.utc)
    start_time_static = datetime.now(timezone.utc)



    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:

        subscribe_message = {"APIKey": "dd18d1daa6f90eada4ab120f6809e0d58ebdfb4c", "BoundingBoxes": [[[83, -32], [30, 109]]]}

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message["MessageType"]

            if message_type == "PositionReport":
                ais_message = message['Message']['PositionReport']
                timestamp=datetime.now(timezone.utc)
                row=[{'time':timestamp,'ship_id':ais_message['UserID'],'latitude':ais_message['Latitude'],
                      'longitude':ais_message['Longitude'],'year':timestamp.year,'month':timestamp.month,'day':timestamp.day,'hour':timestamp.hour,
                      'nav_status':ais_message['NavigationalStatus'],'sog':ais_message['Sog'],'cog':ais_message['Cog'],'true_heading':ais_message['TrueHeading'],'com_state':ais_message['CommunicationState']}]
                
                pos_data.extend(row)
                if len(pos_data)>chunksize_pos:
                    pos_data = pd.DataFrame(pos_data)
                    table = pa.Table.from_pandas(pos_data, schema=schema)
                    pq.write_to_dataset(table,root_path='data/position_reports', schema=schema,partition_cols=['year','month','day','hour'])
                    pos_data = []
                    logger.info(f"chunksize {chunksize_pos} for pos_data reached in {(datetime.now(timezone.utc)-start_time_pos)}")
                    start_time_pos=timestamp
                  
            if message_type == "ShipStaticData":
                ais_message = message['Message']['ShipStaticData']
                timestamp=datetime.now(timezone.utc)
                row=[{'time':timestamp,'year':timestamp.year,'month':timestamp.month,'day':timestamp.day,'hour':timestamp.hour,
                      'ship_id':ais_message['UserID'],'imo_number':ais_message['ImoNumber'],'callsign':ais_message['CallSign'],
                      'name':ais_message['Name'],'type':ais_message['Type'],'fix_type':ais_message['FixType'],
                      'dim_a':ais_message['Dimension']['A'],'dim_b':ais_message['Dimension']['B'],'dim_c':ais_message['Dimension']['C'],'dim_d':ais_message['Dimension']['D'],
                      'eta_month':ais_message['Eta']['Month'],'eta_day':ais_message['Eta']['Day'],'eta_hour':ais_message['Eta']['Hour'],
                      'max_static_draught':ais_message['MaximumStaticDraught'],'destination':ais_message['Destination']}]
                
                static_data.extend(row)
                if len(static_data)>chunksize_static:
                    static_data = pd.DataFrame(static_data)
                    table = pa.Table.from_pandas(static_data, schema=schema_static)
                    pq.write_to_dataset(table,root_path='data/static_ship_data', schema=schema_static,partition_cols=['year','month','day','hour'])
                    static_data = []
                    logger.info(f"chunksize {chunksize_static} for static_data reached in {(datetime.now(timezone.utc)-start_time_static)}")
                    start_time_static=timestamp
                    

                

if __name__ == "__main__":
    start_time_total = datetime.now(timezone.utc)
    pos_data = []
    static_data = []
    fields = [('time',pa.time64('ns')),('ship_id', pa.int64()),('latitude', pa.float64()), ('longitude', pa.float64()),
              ('year', pa.int64()),('month', pa.int64()),('day', pa.int64()),('hour', pa.int64()),
              ('nav_status', pa.int64()),('sog', pa.float64()),('cog', pa.float64()),('true_heading', pa.int64()),('com_state', pa.int64())]
    schema = pa.schema(fields)
    fields_static = [('time',pa.time64('ns')),('year', pa.int64()),('month', pa.int64()),('day', pa.int64()),('hour', pa.int64()),
              ('ship_id', pa.int64()),('imo_number', pa.int64()), ('callsign', pa.string()),
              ('name', pa.string()),('type', pa.int64()),('fix_type', pa.int64()),
              ('dim_a', pa.int64()),('dim_b', pa.int64()),('dim_c', pa.int64()),('dim_d', pa.int64()),
              ('eta_month', pa.int64()),('eta_day', pa.int64()),('eta_hour', pa.int64()),
              ('max_static_draught', pa.float64()),('destination', pa.string())]
    schema_static = pa.schema(fields_static)
    
    try:
        asyncio.run(connect_ais_stream(pos_data,static_data,schema,schema_static))
    except KeyboardInterrupt:
        logger.info("Manual Interruption...")
    except Exception as e:
        logger.info(f"An other error occured: {e}")
        logger.info("Trying to restart...")
        asyncio.run(connect_ais_stream(pos_data,static_data,schema,schema_static))
    finally:
        if pos_data!=0:
            pos_data = pd.DataFrame(pos_data)
            table = pa.Table.from_pandas(pos_data, schema=schema)
            pq.write_to_dataset(table,root_path='data/position_reports', schema=schema,partition_cols=['year','month','day','hour'])
            logger.info(f"Length of {len(pos_data)} for pos_data added while closing")
        if static_data!=0:
            static_data = pd.DataFrame(static_data)
            table = pa.Table.from_pandas(static_data, schema=schema_static)
            pq.write_to_dataset(table,root_path='data/static_ship_data', schema=schema_static,partition_cols=['year','month','day','hour'])
            logger.info(f"Length of {len(static_data)} for static_data added while closing")
        logger.info(f"Length of total execution: {datetime.now(timezone.utc)-start_time_total}")
        logger.info("The program has been safely closed")
