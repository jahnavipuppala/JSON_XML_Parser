import xml.etree.ElementTree as ET
import json
import sqlite3

# Configurable metadata for parsing
metadata = {
    'XML': {
        'root_tag': 'root',
        'item_tag': 'item',
        'attributes': ['id', 'name', 'price']
    },
    'JSON': {
        'key_path': ['products']
    }
}

# Parse XML data based on metadata and store in database
def parse_and_store_xml(xml_data):
    root_tag = metadata['XML']['root_tag']
    item_tag = metadata['XML']['item_tag']
    attributes = metadata['XML']['attributes']

    tree = ET.ElementTree(ET.fromstring(xml_data))
    root = tree.getroot()

    records = []
    for item in root.findall(item_tag):
        record = {attr: item.findtext(attr) for attr in attributes}
        records.append(record)

    store_data_in_database(records)

# Parse JSON data based on metadata and store in database
def parse_and_store_json(json_data):
    key_path = metadata['JSON']['key_path']

    data = json.loads(json_data)
    for key in key_path:
        data = data[key]

    records = data

    store_data_in_database(records)

# Store parsed data in SQLite database
def store_data_in_database(records):
    conn = sqlite3.connect('parsed_data.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS parsed_data
                 (id INTEGER PRIMARY KEY, name TEXT, price REAL)''')

    for record in records:
        c.execute("INSERT INTO parsed_data (name, price) VALUES (?, ?)", (record['name'], record['price']))

    conn.commit()
    conn.close()

# Example XML and JSON data
xml_data = """
<root>
    <item>
        <id>1</id>
        <name>Product A</name>
        <price>100.0</price>
    </item>
    <item>
        <id>2</id>
        <name>Product B</name>
        <price>200.0</price>
    </item>
</root>
"""

json_data = """
{
    "products": [
        {"id": 3, "name": "Product C", "price": 150.0},
        {"id": 4, "name": "Product D", "price": 250.0}
    ]
}
"""

# Parse and store JSON data
parse_and_store_json(json_data)
