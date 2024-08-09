import pandas as pd
import numpy as np
import pymongo as pm
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os


def load_data():
    # Load the data frame
    #df = pd.read_csv('ICRISAT-DLD.csv')
    df = pd.read_csv('archive/Crop_details.csv')

    # print(df.shape)
    return df

def environomental_variables():
    # Get the connection string from the environment
    url = os.environ.get('MONGO')
    return url

def connect_to_mongo(url):
    # Create a new client and connect to the server
    try:
        client = MongoClient(url, server_api=ServerApi('1'))

        print('Connected successfully')
        return client
    except Exception as e:
        print(e)

    return None



def insert_data(client, df):
    # Get the database

    db = client.get_database('agridataset')
    # Create a new collection
    agri_data= db['cropdata']




    # Insert the data frame into the collection
    # The dictionary will be converted to a BSON document
    agri_data.insert_many(df.to_dict('records'))

    print('Data inserted successfully')


def query_data(client):
    db = client.list_database_names()
    print(db)

    tabelas = client['agridataset'].list_collection_names()
    print(tabelas)

    collection = client['agridataset']['agridata']
    print(collection.count_documents({}))

def main():

    # Load the data
    #df = load_data()

    # Get the connection string
    url = environomental_variables()

    # Connect to the server
    client = connect_to_mongo(url)


    if client is None:
        return
    # Insert the data
    #insert_data(client, df)

    # Query the data


    #query_data(client)


if __name__ == '__main__':

    main()
#
