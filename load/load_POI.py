""" Load Datatourisme data and parse it to :
    - create MariaDB tables 
    - create documents to load data into MongoDB 
    - select data to load into Neo4j
    - set data into MariaDB tables
"""

import json
import pathlib
import uuid
from sqlalchemy import select #exc,  create_engine 
from datetime import datetime
from tqdm import tqdm

# path to import "local" modules
#import sys
#sys.path.append('./load/')

from lib.create_DB_ORM import *
from lib.load_maria import connect_maria, find_object_in_list, find_element, \
    create_POI_for_mariadb, load_POI_into_mariadb
from lib.load_neo4j import find_geoloc_POI, load_POI_into_neo4j
from lib.load_mongo import create_POI_for_mongodb, load_POI_into_mongodb

from settings import NEO4J_URI, NEO4J_USER, NEO4J_PWD, MARIADB_DB, MARIADB_HOST, MARIADB_PORT, \
    MARIADB_USER, MARIADB_PWD, MONGODB_URI, MONGODB_DB, MONGODB_POI_COLLECTION, DATAFILES_POI


def load_DATATOURISME_POI(file_dir: str, db_maria_connect: dict, db_mongo_connect: dict, db_neo4j_connect: dict, DEBUG: bool):

    session = connect_maria(db_maria_connect=db_maria_connect, DEBUG=DEBUG)

    # existing lists in MariaDB
    poi_dic=session.execute(select(Poi)).all()
    poi_type_dic=session.execute(select(PoiType)).all()
    poi_theme_dic=session.execute(select(PoiTheme)).all()
    audience_dic=session.execute(select(TargetAudience)).all()

    
    # raw_data : filenames
    desktop = pathlib.Path(file_dir)
    files = list(desktop.rglob("*.json"))

    l_neo4j, l_mongodb = [], []
    l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience = [], [], [], []
    
    # loop on all the indivual .json files
    for file in tqdm(files):

        # load data from file to variable
        with open(file) as f:
                POI = json.load(f)

        # request in MariaDB to look for an existing POI
        #with Session(engine) as session:
        try:
            datatourisme_id = POI['@id']
            res = find_object_in_list(datatourisme_id, poi_dic, 'poi')
            
            # new POI to create and upload in mariaDB
            if res == None:

                # UUID for 1 POI
                UUID_gen = str(uuid.uuid4())
                
                poi_dic, poi_type_dic, poi_theme_dic, audience_dic, \
                    l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience = \
                    create_POI_for_mariadb(POI, UUID_gen, datatourisme_id, \
                                            poi_dic, poi_type_dic, poi_theme_dic, audience_dic, \
                                            l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience)
                
                l_mongodb.append(create_POI_for_mongodb(POI, UUID_gen))
                l_neo4j.append(find_geoloc_POI(POI, UUID_gen, "POI"))
                
            # POI to update (one or more update values)
            elif res != None and res.LAST_UPDATE != find_element(POI, 'lastUpdate') :    
                print("This POI has to be updated (code to develop)")

            # already existing POI
            else:
                print("This POI is already in the database and up to date.")

        except KeyError:
            pass  


    # Load to mariadb
    print('loading to mariadb...')
    load_POI_into_mariadb(session, l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience)
    print('- loaded', len(l_mariadb_poi), 'rows in maria table')

    # Load to mongodb
    print('loading to mongodb...')
    load_POI_into_mongodb(l_mongodb, db_mongo_connect)
    print('- loaded', len(l_mongodb), 'documents in mongo collection')
    
    # Load to neo4j
    print('loading to neo4j...')
    load_POI_into_neo4j(l_neo4j, db_neo4j_connect)
    print('- loaded', len(l_neo4j), 'points in neo4j database')
    

        
if __name__ == '__main__':

    file_dir = DATAFILES_POI
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print("Start load POI: ", timestamp)
    
    DEBUG = False

    # parameters to connect MariaDB
    db_maria_connect = {
        'db_host': MARIADB_HOST,
        'db_port': MARIADB_PORT,
        'db_user': MARIADB_USER,
        'db_password': MARIADB_PWD,
        'db_name': MARIADB_DB
    }

    # parameters to connect to MongoDB
    db_mongo_connect = {
        'uri': MONGODB_URI,
        'database': MONGODB_DB, 
        'collection': MONGODB_POI_COLLECTION,
    }

    # parameters to connect to Neo4j
    db_neo4j_connect = {
        'uri': NEO4J_URI,  
        'username': NEO4J_USER,     
        'password': NEO4J_PWD
    }
    
    # load_DATATOURISME_POI(
    #     file_dir=file_dir, 
    #     db_maria_connect=db_maria_connect, 
    #     db_mongo_connect=db_mongo_connect,
    #     db_neo4j_connect=db_neo4j_connect, 
    #     DEBUG=DEBUG)
    
    timestamp2 = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print("End load POI: ", timestamp2)