from typing import Union
import json
import random

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from fastapi import FastAPI

app = FastAPI()

URL_CLIENT = "mongodb://localhost:27017/"
CLIENT = MongoClient(URL_CLIENT,  server_api=ServerApi('1'))

DB_NAME = "MAIN_DMI_TESTING"
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

def add_parent_id(values, parent_random_id, key):
    if not isinstance(values, dict):
        dictionary = {}
        dictionary[key] = values
        return dictionary
    values["parent_random_id"] = parent_random_id
    return values

def recursive_configuration(parsed_data, metadata_tables, base_table):
    to_insert = {}
    random_id = random.random()
    to_insert["random_id"] = random_id
    print(metadata_tables)
    for key, values in parsed_data.items():
        print(key)
        if isinstance(values, dict):
            recursive_configuration(add_parent_id(values, random_id, key), metadata_tables, base_table)
        if key in metadata_tables["ONE_TO_ONE"]:
            print("KEY", key)
            recursive_configuration(add_parent_id(values, random_id, key), metadata_tables, key)
        elif key in metadata_tables["ONE_TO_MANY"]:
            for child in values:
                recursive_configuration(add_parent_id(child, random_id, key), metadata_tables, key)
        else:
            if key in metadata_tables["MANDATORY"]:
                raise KeyError("Mandatory Key Not present in metadata")
            to_insert[key] = copy_values_if_dict(values)
    insert_one_in_mongo_db(to_insert, base_table)
    return to_insert

@app.post("/parse_json")
async def parse_json(json_data: str):
    parsed_json = json.loads(json_data)
    return parsed_json 

@app.post("/parse_json_and_store_in_mongo_db")
async def parse_json(json_data: str, metadata_tables: dict):
    print("In api")
    parsed_json = json.loads(json_data)
    recursive_configuration(parsed_json, metadata_tables, base_table = BASE_TABLE)
    return parsed_json 

