from typing import Union
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from fastapi import FastAPI

app = FastAPI()

URL_CLIENT = "mongodb://localhost:27017/"
CLIENT = MongoClient(URL_CLIENT,  server_api=ServerApi('1'))

DB_NAME = "sample"
DB = CLIENT[DB_NAME]

def insert_one_in_mongo_db(parsed_data, collection_name, db = DB):
    collection = db[collection_name]
    return collection.insert_one(parsed_data)

BASE_TABLE = "base"
METADATA_TABLES = dict({
    "ONE_TO_MANY": {
        "A_KEY": "",
        "B_KEY": "",
    },
    "ONE_TO_ONE": {
        "C_KEY": "" ,
    },
    MANDATORY: "A", "B"
})


def recursive_configuration(parsed_data, metadata_tables):
    for key, values in parsed_data.items():
        if key in metadata_tables["ONE_TO_ONE"]:
            recursive_configuration(values, metadata_tables)
        insert_one_in_mongo_db(values.copy(), key)

@app.post("/parse_json")
async def parse_json(json_data: str):
    parsed_json = json.loads(json_data)
    return parsed_json 

@app.post("/parse_json_and_store_in_mongo_db")
async def parse_json(json_data: str, metadata_tables: dict = METADATA_TABLES):
    parsed_json = json.loads(json_data)
    recursive_configuration(parsed_json, metadata_tables)
    #store_data_in_mongo_db(parsed_json, "key_name")
    return parsed_json 
