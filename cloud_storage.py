from google.cloud import storage
import json
import os

def upload_to_storage(data: dict):

    bucket = 'vehicles_current_status'
    file_name = 'process1_file.json'
    
    ## instane of the storage client
    storage_client = storage.Client()

    ## instance of a bucket in your google cloud storage
    bucket = storage_client.get_bucket(bucket)

    ## if there already exists a file
    blob = bucket.get_blob(file_name)

    ## uploading data using upload_from_string method
    ## json.dumps() serializes a dictionary object as string
    blob.upload_from_string(json.dumps(data))
    
    #Log
    print('Saved in cloud storage')