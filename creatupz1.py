import boto3
import json
import io
import pandas as pd
import pyarrow.parquet as pq
from geopy.geocoders import Nominatim
import pandas as pd
import numpy as np

def UPZ(lat, lon):
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

    df = pd.read_csv(io.BytesIO(content))

    upz = np.array([UPZ(lat, lon) for lat, lon in zip(df['LATITUDEORI'], df['LONGITUDEORI'])], dtype=object)
    upz = pd.DataFrame({'UPZ': upz})

    df = pd.concat([df, upz], axis=1)

    df.to_csv('mi_archivo.csv', index=False)

    s3_client.put_object(Bucket=target_bucket, Key=target_key, Body=open('mi_archivo.csv', 'rb'))

    return {
        'statusCode': 200,
        'body': json.dumps('Proceso completado exitosamente.')
    }
