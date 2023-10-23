from neo4j import GraphDatabase


def find_geoloc(POI):
    latitude, longitude = None, None
    try:
        latitude = float(POI['isLocatedAt']['schema:geo']['schema:latitude']['@value'])
        longitude = float(POI['isLocatedAt']['schema:geo']['schema:longitude']['@value']) 
        # in the same try as if one is missing, the geoloc data is not interesting anymore
    except KeyError:
        return None, None
    return latitude, longitude


def find_geoloc_POI(POI, UUID_gen, node_label):
    latitude, longitude = None, None
    try:
        latitude = float(POI['isLocatedAt'][0]['schema:geo']['schema:latitude'])
        longitude = float(POI['isLocatedAt'][0]['schema:geo']['schema:longitude'])
        # in the same try as if one is missing, the geoloc data is not interesting anymore
        # here's a different code when POIs are in different json files.
    except KeyError:
        return {'latitude':None, 'longitude': None, 'UUID_gen': UUID_gen, 'node_label': node_label}
    return {'latitude':latitude, 'longitude': longitude, 'UUID_gen': UUID_gen, 'node_label': node_label}


def create_POI_into_neo4j(POI: dict, UUID_gen: str, db_neo4j_connect: dict, node_label: str):
    latitude, longitude = find_geoloc(POI)
    # upload to neo4j
    if latitude is not None and longitude is not None:
        with GraphDatabase.driver(
            db_neo4j_connect['uri'],
            auth=(db_neo4j_connect['username'],
                  db_neo4j_connect['password'])
        ) as driver:
            with driver.session() as session:
                query = (
                    f"MERGE (r:{node_label} {{LONGITUDE: '{longitude}', LATITUDE: '{latitude}', uuid: '{UUID_gen}'}})"
                )
                session.run(query)


def load_POI_into_neo4j(POI_list, db_neo4j_connect): 

    # upload to neo4j
    if POI_list != []:
        with GraphDatabase.driver(db_neo4j_connect['uri'], auth=(db_neo4j_connect['username'], db_neo4j_connect['password'])) as driver:
            with driver.session() as session:
                for POI in POI_list: 
                    latitude, longitude, UUID_gen, node_label = \
                        POI['latitude'], POI['longitude'], POI['UUID_gen'], POI['node_label']
                
                    if latitude != None and longitude != None:                        
                        query = (
                                f"MERGE (r:{node_label} {{LONGITUDE: '{longitude}', LATITUDE: '{latitude}', uuid: '{UUID_gen}'}})"
                            )
                        session.run(query)
