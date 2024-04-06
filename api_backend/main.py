from typing import Union
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from fastapi import FastAPI

app = FastAPI()

URL_CLIENT = "mongodb://localhost:27017/"
CLIENT = MongoClient(URL_CLIENT,  server_api=ServerApi('1'))

DB_NAME = "hack_testing"
DB = CLIENT[DB_NAME]

def insert_one_in_mongo_db(parsed_data, collection_name, db = DB):
    collection = db[collection_name]
    return collection.insert_one(parsed_data)

BASE_TABLE = "base"

def copy_values_if_dict(values):
    if isinstance(values, dict):
        copy = {}
        for key in values:
            copy[key] = values[key]
        return copy
    return values

def recursive_configuration(parsed_data, metadata_tables, base_table):
    print("function called", base_table)
    to_insert = {}
    for key, values in parsed_data.items():
        print("key", key, "value", values)
        if key in metadata_tables["ONE_TO_ONE"]:
            print("make new table")
            recursive_configuration(values, metadata_tables, key)
        else:
            if key in metadata_tables["MANDATORY"]:
                raise KeyError("Mandatory Key Not present in metadata")
            to_insert[key] = copy_values_if_dict(values)
    print("to_insert", to_insert)
    insert_one_in_mongo_db(to_insert, base_table)
    return to_insert

@app.post("/parse_json")
async def parse_json(json_data: str):
    parsed_json = json.loads(json_data)
    return parsed_json 

@app.post("/parse_json_and_store_in_mongo_db")
async def parse_json(json_data: str, metadata_tables: dict):
    print(metadata_tables)
    parsed_json = json.loads(json_data)
    recursive_configuration(parsed_json, metadata_tables, base_table = BASE_TABLE)
    return parsed_json 

