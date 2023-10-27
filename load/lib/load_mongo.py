"""
Module to import data from a json file into mongoDB
"""

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
import json

def retrieve_mongo_data_POI(POI):
    """function that return dictionnary
    reformatting of label, description, and short description
    dictionaries for Mongo injection
    not all POIs necessarily have the appropriate fields.
    """
    label = {}
    try:
        POI_label = POI["rdfs:label"]
        for item in POI_label:
            langue = item['@language']
            valeur = item['@value']
            label[langue] = valeur
    except KeyError:
        pass
    except TypeError:
        # sometimes it's juste a dictionnary and not a list
        label[POI_label['@language']] = POI_label['@value']

    ShortDesc = {}
    try:
        POI_ShortDesc = POI["owl:topObjectProperty"]["shortDescription"]
        for item in POI_ShortDesc:
            langue = item['@language']
            valeur = item['@value']
            ShortDesc[langue] = valeur
    except KeyError:
        pass
    except TypeError:
        # sometimes it's juste a dictionnary and not a list
        ShortDesc[POI_ShortDesc['@language']] = POI_ShortDesc['@value']

    Desc = {}
    try:
        POI_Desc = POI['owl:topObjectProperty']['dc:description']
        for item in POI_Desc:
            langue = item['@language']
            valeur = item['@value']
            Desc[langue] = valeur
    except KeyError:
        pass
    except TypeError:
        # sometimes it's juste a dictionnary and not a list
        Desc[POI_Desc['@language']] = POI_Desc['@value']

    return {"LABEL": label, "SHORT_DESCRIPTION": ShortDesc,
            "DESCRIPTION": Desc}


def load_file_mongo(mongo_host, mongo_port, db_username, db_password, db_name,
                    collection_name, data_file):

    client = MongoClient(
        host=mongo_host,
        port=mongo_port,
        username=db_username,
        password=db_password
    )
    db = client.get_database(db_name)

    try:
        collection = db.create_collection(name=collection_name)
    except CollectionInvalid:
        collection = client[db_name][collection_name]
    # print(client.list_database_names())
    with open(data_file) as f:
        data = json.load(f)
    collection.insert_many(data)


def find_element_to_dic(POI, element):
    res = {}
    try:
        if element == 'label':
            dict = POI['rdfs:label']
        elif element == 'shortDesc':
            dict = POI['hasDescription'][0]['shortDescription']
        elif element == 'desc':
            dict = POI['hasDescription'][0]['dc:description']

        for item in dict.keys():
            language = item
            value = dict[item][0]
            res[language] = value

    except KeyError:
        return {}

    except TypeError:
        # sometimes it's just a dictionary and not a list
        res[dict['@language']] = dict['@value']  # to be reviewed

    return res


def create_POI_for_mongodb(POI, UUID_gen):

    label = find_element_to_dic(POI, 'label')
    shortDesc = find_element_to_dic(POI, 'shortDesc')
    desc = find_element_to_dic(POI, 'desc')

    document = {
        "UUID": UUID_gen,
        "LABEL": label,
        "SHORT_DESCRIPTION": shortDesc,
        "DESCRIPTION": desc
    }

    return document


def load_POI_into_mongodb(POI_list, db_mongo_connect):

    if POI_list != []:
        collection = MongoClient(db_mongo_connect['uri'])[
            db_mongo_connect['database']][db_mongo_connect['collection']]

        collection.insert_many(POI_list)


if __name__ == '__main__':
    mongo_host = "localhost"
    mongo_port = 27017
    db_username = "admin"
    db_password = "admin"
    db_name = "datatourisme"
    collection_name = "test"
    data_file = "output_files/collection_DATATOURISME_TAIL_2023-09-15_15-34-08.json"

    load_file_mongo(mongo_host=mongo_host, mongo_port=mongo_port,
                    db_username=db_username, db_password=db_password,
                    db_name=db_name, collection_name=collection_name,
                    data_file=data_file)
