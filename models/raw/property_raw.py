import os
import json
import pandas as pd
import numpy as np
from google.cloud import storage

def model(dbt, session):
    storage_client = storage.Client()
    bucket = storage_client.bucket('rightmove-data-bucket')
    blobs = storage_client.list_blobs(bucket)
    data_dict = dict()

    for blob_index, blob in enumerate(blobs):
        with blob.open("r", encoding='utf-8-sig') as j:
            content = json.load(j)
            columns = content["columns"]
            rows = content["data"]
            if blob_index == 0:
                for column_index, column in enumerate(columns):
                    data_dict.update({str(column): list()})
            for row in rows:
                for column_index, column in enumerate(columns):
                    if row[column_index] == '':
                        row[column_index] = np.nan
                    data_dict[column].append(row[column_index])
    df = pd.DataFrame(data_dict)
    return df