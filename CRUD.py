import pandas as pd
import numpy as np
import pymongo as pm
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from bson import ObjectId


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

    # if datbase is already exist then it will connect to that database
    # otherwise it will create a new database

    db = client.get_database('agridataset')
    # Create a new collection
    agri_data= db['cropdata']




    # Insert the data frame into the collection
    # The dictionary will be converted to a BSON document
    agri_data.insert_many(df.to_dict('records'))

    #Insert One
    #drop_collection

    print('Data inserted successfully')







def query_data(client,obj_id=ObjectId('66b667e44c5540a5c275fdde')):#searching the data
    db = client.list_database_names() # list all the database
    print(db)

    tabelas = client['agridataset'].list_collection_names() # list all the collection
    print(tabelas)

    collection = client['agridataset']['agri_data'] # get the collection

    # Query the data
    query = collection.find({'_id':obj_id})# pass {} only to search all

    for i in query:
        print(list(i.items())[:2])

    # find_one
    # find return the cursor object

    # when you are facing problem to 20000 ms conection time out the check you network access
    # if you are using the cloud then check the network access


def update_data(client):
    db = client.get_database('agridataset')
    collection = db['agri_data']

    # Update the data
    obj_id = ObjectId('66b667e44c5540a5c275fdde')

    collection.update_one({'_id': obj_id}, {'$set': {'Dist Code': 1}})

    print('Data updated successfully')
    return obj_id

    #update_one for update only one item in the jason or data base
    #update_many for update all the item in the jason or data base meanse more thane one



def delete_data(client):
    db = client.get_database('agridataset')
    collection = db['agri_data']

    # Delete the data
    obj_id = ObjectId('66b667e44c5540a5c275fde4')

    collection.delete_one({'_id': obj_id})

    print('Data deleted successfully')
    return obj_id

    #delete_one for delete only one item in the jason or data base
    #delete_many for delete all the item in the jason or data base meanse more thane one


def replace_data(client):
    db = client.get_database('agridataset')
    collection = db['agri_data']

    # Replace the data
    obj_id = ObjectId('66b667e44c5540a5c275fddf')

    collection.replace_one({'_id': obj_id}, {'Dist Code': 5})

    print('Data replaced successfully')

    return obj_id

    #replace_one for replace only one item in the jason or data base
    #replace_many for replace all the item in the jason or data base meanse more thane one


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

    # Query the data / search the data
    query_data(client)

    # Update the data
    obj_id_update = update_data(client)

    query_data(client,obj_id_update)
    # Delete the data
    #
    obj_id_del = delete_data(client)

    query_data(client,obj_id_del)

    # Replace the data
    obj_repl =  replace_data(client)

    query_data(client,obj_repl)



if __name__ == '__main__':

    main()
#
