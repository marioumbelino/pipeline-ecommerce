from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongodb(uri):

    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)
        
def connect_database(client, db_name):
    # connect (or create) the database
    db = client[db_name]
    return db

def connect_collection(db, column_name):
    # connect (or create) the collection
    collection = db[column_name]
    return collection

def extract_data_api(url):
    # request to get data from an API
    response = requests.get(url)

    if response.status_code == 200: # if request sucessfully
        return response.json()
    else:
        raise Exception(f'Error - Request status code: {response.status_code}')
    
def insert_data(column, data):
    # insert data to MongoDB
    if len(data) == 1:
        return column.insert_one(data)
    elif len(data) > 1:
        return column.insert_many(data)
    else:
        raise Exception('Sorry, looks like the data is empty')

if __name__ == '__main__':
    # access mongo uri
    with open('data_base_key.txt', 'r') as file:
        file_content = file.read()
        
    client = connect_mongodb(file_content) # connect to mongo
    database = connect_database(client, 'test') # connect or create the database
    collection = connect_collection(database, 'collumn_test') # connect or create the collection

    # access the API and extract its data
    data_content = extract_data_api('https://labdados.com/produtos')

    # saves the data to a MongoDB database 
    docs = insert_data(collection, data_content)
