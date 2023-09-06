import boto3
import json
import io
import pandas as pd
import pyarrow.parquet as pq
from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np

def upz(lat, lon):
  try:
    return geolocator.reverse((lat, lon)).raw['address']['neighbourhood']
  except:
    return None

s3_client = boto3.client('s3')
source_bucket = 'semilleros1'
source_key = 'dataset0.csv'
target_bucket = "semilleros2"
target_key = "dataset0.csv"
geolocator = Nominatim(user_agent="my_app")

def crear():
  response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
  content = response['Body'].read()
  #table = pq.read_table(io.BytesIO(content))
  #df = table.to_pandas()
  df = pd.read_parquet(content)
  upz = np.array([upz(lat, lon) for lat, lon in zip(df['LATITUDEORI'], df['LONGITUDEORI'])], dtype=object)
  upz = pd.DataFrame({'UPZ': upz})
  df = pd.concat([df, upz], axis=1)
  modified_table = pq.Table.from_pandas(df)
  output_stream = io.BytesIO()
  pq.write_table(modified_table, output_stream)
  s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=output_stream.getvalue())
  return {
        'statusCode': 200,
        'body': json.dumps('Proceso completado exitosamente.')
    }
