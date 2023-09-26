from sqlalchemy import create_engine, exc
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session
from lib.create_DB_ORM import Base, Poi, PoiType, PoiTheme, TargetAudience
import uuid


def connect_maria(db_maria_connect: dict, DEBUG: bool):
    db_user = db_maria_connect["db_user"]
    db_password = db_maria_connect["db_password"]
    db_host = db_maria_connect["db_host"]
    db_port = db_maria_connect["db_port"]
    db_name = db_maria_connect["db_name"]

    db_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(db_url, echo=DEBUG)
    if not database_exists(engine.url):
        create_database(engine.url)
    session = Session(engine)

    # creation of tables if not already exists into BDD MariaDB
    Base.metadata.create_all(engine)

    return session


def find_object_in_list(object_name, list_of_names, object_type):
    res=None
            
    if object_type == "poi_type" or object_type == "poi_theme" or object_type == "audience" : 
        for a in list_of_names:
            if object_name == a[0].NAME: 
                res=a[0]
                break   
        return res
    
    elif object_type == "poi": 
        for a in list_of_names:
            if object_name == a[0].DATATOURISME_ID: 
                res=a[0]
                break
        return res


def find_element(POI, element):
    res = None
    try:
        match element:
            case "petsAllowed": 
                res = POI['hasFeature'][0]['petsAllowed']
            case  "reducedMobilityAccess": 
                res = POI['reducedMobilityAccess']
            case "webpage": 
                res = POI['isOwnedBy'][0]['foaf:homepage'][0]
            case "image": 
                res = POI['hasMainRepresentation'][0]['ebucore:hasRelatedResource'][0]['ebucore:locator'][0]
            case "city": 
                res = POI['isLocatedAt'][0]['schema:address'][0]['schema:addressLocality']
            case "postalCode": 
                res = POI['isLocatedAt'][0]['schema:address'][0]['schema:postalCode']
            case "postalAddress": 
                res = POI['isLocatedAt'][0]['schema:address'][0]['schema:streetAddress'][0]
            case "lastUpdate": 
                res = POI['lastUpdateDatatourisme']
        
    except KeyError:
        return None

    return res


def create_POI_for_mariadb(POI, UUID_gen, datatourisme_id, 
                            poi_dic, poi_type_dic, poi_theme_dic, audience_dic, 
                            l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience): 
         
    pets_allowed = find_element(POI, 'petsAllowed')
    reduced_mobility_access = find_element(POI, 'reducedMobilityAccess')
    webp = find_element(POI, 'webpage')
    image = find_element(POI, 'image')
    city = find_element(POI, 'city')
    postalCode = find_element(POI, 'postalCode')
    postalAddress = find_element(POI, 'postalAddress')
    lastUpdate = find_element(POI, 'lastUpdate')

    # with possibles associations with other tables
    poi_types, poi_themes, target_audience = [], [], []

    try:
        for p_type in POI['@type']:
            poi_type_res = find_object_in_list(p_type, poi_type_dic, 'poi_type')
            if poi_type_res == None:  
                poi_type_new = PoiType(NAME=p_type)
                
                #append in lists
                poi_types.append(poi_type_new)
                l_mariadb_poi_type.append(poi_type_new)
                poi_type_dic.append([poi_type_new,])
            else:
                #append in list existing poi_type
                poi_types.append(poi_type_res)
    except KeyError:
        pass

    try:
        p_theme = POI['hasTheme'][0]['@id'][3:]
        poi_theme_res = find_object_in_list(p_theme, poi_theme_dic, 'poi_theme')    
        
        if poi_theme_res == None:    
            poi_theme_new = PoiTheme(NAME=p_theme)
                
            #append in lists
            poi_themes.append(poi_theme_new)
            l_mariadb_poi_theme.append(poi_theme_new)
            poi_theme_dic.append([poi_theme_new,])

        else:
            poi_themes.append(poi_theme_res)

    except KeyError:
        pass

    try:
        audience = POI['hasDescription'][0]["isDedicatedTo"][0]["@id"][3:]
        target_audience_res = find_object_in_list(audience, audience_dic, 'audience')
                
        if target_audience_res == None:
            id = str(uuid.uuid4())
            target_audience_new = TargetAudience(id=id, NAME=audience)
                    
            target_audience.append(target_audience_new)
            l_mariadb_audience.append(target_audience_new)
            audience_dic.append([target_audience_new,])               
            
        else:
            target_audience.append(target_audience_res)

    except KeyError:
        pass

    new_poi = Poi(id=UUID_gen, 
        PETS_ALLOWED = pets_allowed,
        REDUCED_MOBILITY_ACCESS = reduced_mobility_access, 
        WEBPAGE_LINK=webp, 
        IMAGE_LINK=image,
        CITY=city, 
        POSTAL_CODE=postalCode, 
        POSTAL_ADDRESS=postalAddress, 
        DATATOURISME_ID=datatourisme_id, 
        LAST_UPDATE = lastUpdate)

    new_poi.POI_TYPES=poi_types
    new_poi.POI_THEMES=poi_themes
    new_poi.TARGET_AUDIENCE=target_audience

    poi_dic.append([new_poi,0])
    l_mariadb_poi.append(new_poi)

    
    return poi_dic, poi_type_dic, poi_theme_dic, audience_dic, \
        l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience


def load_POI_into_mariadb(session, l_mariadb_poi, l_mariadb_poi_type, l_mariadb_poi_theme, l_mariadb_audience):

    #with Session(engine) as session:
    try:
        for poi_type in l_mariadb_poi_type: 
            session.add(poi_type)
    except exc.IntegrityError:
        session.rollback()
        print("Integrity error on POI TYPE: it already exists.")

    try:
        for poi_theme in l_mariadb_poi_theme: 
            session.add(poi_theme)
    except exc.IntegrityError:
        session.rollback()
        print("Integrity error on POI THEME: it already exists.")

    try:
        for audience in l_mariadb_audience: 
            session.add(audience)
    except exc.IntegrityError:
        session.rollback()
        print("Integrity error on TARGET AUDIENCE: it already exists.")
    
    try:
        for poi in l_mariadb_poi: 
            session.add(poi)
    except exc.IntegrityError:
        session.rollback()
        print("Integrity error on POI : it already exists")
    
    session.commit()
