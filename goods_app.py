"""считывание json-файла и заполнение таблицы товарами."""
import jsonschema
from jsonschema import validate
import json
from typing import Any
import sqlite3

conn = sqlite3.connect('task.db')


def validate_file(f: dict) -> Any:
    """валидация json-файла."""
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "$id": "http://example.com/example.json",
        "type": "object",
        "title": "The root schema",
        "description": "The root schema comprises the entire JSON document.",
        "default": {},
        "examples": [
            {
                "id": 123,
                "name": "Телевизор",
                "package_params": {
                    "width": 5,
                    "height": 10
                },
                "location_and_quantity": [
                    {
                        "location": "Магазин на Ленина",
                        "amount": 7
                    },
                    {
                        "location": "Магазин в центре",
                        "amount": 3
                    }
                ]
            }
        ],
        "required": [
            "id",
            "name",
            "package_params",
            "location_and_quantity"
        ],
        "additionalProperties": False,
        "properties": {
            "id": {
                "$id": "#/properties/id",
                "type": "integer",
                "title": "The id schema",
                "description": "An explanation about the purpose of this instance.",
                "default": 0,
                "examples": [
                    123
                ]
            },
            "name": {
                "$id": "#/properties/name",
                "type": "string",
                "title": "The name schema",
                "description": "An explanation about the purpose of this instance.",
                "default": "",
                "examples": [
                    "Телевизор"
                ]
            },
            "package_params": {
                "$id": "#/properties/package_params",
                "type": "object",
                "title": "The package_params schema",
                "description": "An explanation about the purpose of this instance.",
                "default": {},
                "examples": [
                    {
                        "width": 5,
                        "height": 10
                    }
                ],
                "required": [
                    "width",
                    "height"
                ],
                "additionalProperties": True,
                "properties": {
                    "width": {
                        "$id": "#/properties/package_params/properties/width",
                        "type": "integer",
                        "title": "The width schema",
                        "description": "An explanation about the purpose of this instance.",
                        "default": 0,
                        "examples": [
                            5
                        ]
                    },
                    "height": {
                        "$id": "#/properties/package_params/properties/height",
                        "type": "integer",
                        "title": "The height schema",
                        "description": "An explanation about the purpose of this instance.",
                        "default": 0,
                        "examples": [
                            10
                        ]
                    }
                }
            },
            "location_and_quantity": {
                "$id": "#/properties/location_and_quantity",
                "type": "array",
                "title": "The location_and_quantity schema",
                "description": "An explanation about the purpose of this instance.",
                "default": [],
                "examples": [
                    [
                        {
                            "location": "Магазин на Ленина",
                            "amount": 7
                        },
                        {
                            "location": "Магазин в центре",
                            "amount": 3
                        }
                    ]
                ],
                "additionalItems": True,
                "items": {
                    "anyOf": [
                        {
                            "$id": "#/properties/location_and_quantity/items/anyOf/0",
                            "type": "object",
                            "title": "The first anyOf schema",
                            "description": "An explanation about the purpose of this instance.",
                            "default": {},
                            "examples": [
                                {
                                    "location": "Магазин на Ленина",
                                    "amount": 7
                                }
                            ],
                            "required": [
                                "location",
                                "amount"
                            ],
                            "additionalProperties": True,
                            "properties": {
                                "location": {
                                    "$id": "#/properties/location_and_quantity/items/anyOf/0/properties/location",
                                    "type": "string",
                                    "title": "The location schema",
                                    "description": "An explanation about the purpose of this instance.",
                                    "default": "",
                                    "examples": [
                                        "Магазин на Ленина"
                                    ]
                                },
                                "amount": {
                                    "$id": "#/properties/location_and_quantity/items/anyOf/0/properties/amount",
                                    "type": "integer",
                                    "title": "The amount schema",
                                    "description": "An explanation about the purpose of this instance.",
                                    "default": 0,
                                    "examples": [
                                        7
                                    ]
                                }
                            }
                        }
                    ],
                    "$id": "#/properties/location_and_quantity/items"
                }
            }
        }
    }
    try:
        validate(f, schema)
    except jsonschema.exceptions.ValidationError:
        return False
    return True


def load_data(path:str) -> dict:
    """загрузка json-файла."""
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)  # загружаем из файла данные в словарь data
        return data


def save(di: dict) -> None:
    """заполнение таблицы товарами."""
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS goods ( id integer PRIMARY KEY AUTOINCREMENT,
    name varchar not null, package_height float not null,
    package_width float not null );""")
    c.execute("""CREATE TABLE IF NOT EXISTS shops_goods (id integer PRIMARY KEY AUTOINCREMENT,
    id_good integer not null, location varchar not null,
    amount integer not null,
    FOREIGN KEY (id_good)  REFERENCES goods (id));""")
    for key, value in di.items():
        if key == 'id':
            id = value
        if key == 'name':
            name = value
        if key == 'package_params':
            width = value['width']
            height = value['height']
        if key == 'location_and_quantity':
            loc = list()
            am = list()
            for i in value:
                loc.append(i['location'])
                am.append(i['amount'])
    if f"""SELECT * FROM GOODS WHERE ID={id}""":
        c.execute(
            f"""UPDATE goods SET name='{name}', package_height='{height}',package_width='{width}' where id={id};""")
        for j in range(len(loc)):
            c.execute(
                f"""UPDATE shops_goods set amount='{am[j]}' where id_good={id} and location='{loc[j]}';""")
        conn.commit()
        return
    c.execute(f"""INSERT INTO goods (id, name, package_height,package_width)
    VALUES ('{id}','{name}','{height}','{width}');""")
    for j in range(len(loc)):
        c.execute(
            f"""INSERT INTO shops_goods (id_good, location, amount) VALUES ('{id}','{loc[j]}','{am[j]}');""")
    conn.commit()


d = load_data('loaded_file.json')
if validate_file(d):
    print("Validation completed. Correct file.")
    save(d)
else:
    print("Некорректный json-файл")
conn.close()
