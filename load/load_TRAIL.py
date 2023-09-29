""" Load datatourisme data and parse it to :
    - create MariaDB Tables
    - create json file to load data into mongoDB and Noe4J
    - set datas into MariaDB Tables
"""

import json
import uuid
from lib.create_DB_ORM import Trail
from lib.annex_table import retrieve_annex_modality, create_tour_type,\
    create_trail_type, create_theme, create_audience, create_trailViz
from lib.load_mongo import retrieve_mongo_data_POI, load_POI_into_mongodb
from lib.load_neo4j import create_POI_into_neo4j
from lib.load_maria import connect_maria
from sqlalchemy import exc, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError
import re
from datetime import datetime

from settings import MARIADB_DB, MARIADB_HOST, MARIADB_PORT, \
    MARIADB_USER, MARIADB_PWD, DATAFILES_TRAIL, MONGODB_TRAIL_COLLECTION, MONGODB_URI, MONGODB_DB, \
    NEO4J_URI, NEO4J_USER, NEO4J_PWD


def retrieve_all_trails(session: Session):
    return session.execute(select(Trail)).all()


def update_trail(old_trail, POI):
    """function to update an already existed trail"""
    try:
        if old_trail.LAST_UPDATE == POI['lastUpdate']['@value']:
            pass
        else:
            print("POI must be updated... Code to do")
    except KeyError:
        print(f"No update date into POI {POI}")


def create_trail(POI):
    """function to create new trail without:
                    TOUR_TYPE,
                    TRAIL_TYPE,
                    THEME,
                    TARGET_AUDIENCE,
                    TRAIL_VISUALIZATION
    """
    UUID_gen = str(uuid.uuid4())
    try:
        pets_allowed = POI['petsAllowed']['@value']
    except KeyError:
        pets_allowed = None

    try:
        duration = POI['duration']['@value']
    except KeyError:
        duration = 0

    try:
        distance = POI['tourDistance']['@value']
    except KeyError:
        distance = 0

    try:
        image = POI['hasMainRepresentation']['ebucore:hasRelatedResource']['ebucore:locator']['@value']
    except KeyError:
        image = ''

    try:
        city = POI['isLocatedAt']['schema:address']['schema:addressLocality']
        if isinstance(city, list):
            city = ','.join(city)
    except KeyError:
        city = ''

    try:
        postalCode = POI['isLocatedAt']['schema:address']['schema:postalCode']
    except KeyError:
        postalCode = None

    try:
        postalAddress = POI['isLocatedAt']['schema:address']['schema:streetAddress']
        if isinstance(postalAddress, list):
            postalAddress = ','.join(postalAddress)
    except KeyError:
        postalAddress = ''

    try:
        lastupdate = POI['lastUpdate']['@value']
    except KeyError:
        lastupdate = None

    new_trail = Trail(id=UUID_gen,
                      DataTourism_ID=POI['@id'],
                      PETS_ALLOWED=pets_allowed,
                      DURATION=duration,
                      DISTANCE=distance,
                      IMAGE_LINK=image,
                      CITY=city,
                      POSTAL_CODE=postalCode,
                      POSTAL_ADDRESS=postalAddress,
                      LAST_UPDATE=lastupdate
                      )

    return new_trail


def append_trail(session: Session, annex_type: str, annex_field: str, list_mod: list, list_obj: list, new_trail: Trail, DEBUG: bool):
    """function to create/add annex foreignkey if needed to trail
    annex_type = "tourType", "trailType", "theme" or "audience"
        return:
            - bool to indicate if new modality created,
            - modality object
            - list of object updated
            - list of modality updated
    """
    mat = {
        "tourType": {
            "create_func": create_tour_type,
            # "attribut": "TYPE",
            "trail_attribut": "TOUR_TYPE"},
        "trailType": {
            "create_func": create_trail_type,
            # "attribut": "TYPE",
            "trail_attribut": "TRAIL_TYPE"},
        "theme": {
            "create_func": create_theme,
            # "attribut": "THEME",
            "trail_attribut": "THEME"},
        "audience": {
            "create_func": create_audience,
            # "attribut": "AUDIENCE",
            "trail_attribut": "TARGET_AUDIENCE"}
        }

    created = False
    trail_attribut = getattr(new_trail, mat[annex_type]["trail_attribut"])
    # attribut = mat[annex_type]["attribut"]

    if annex_field in list_mod:
        obj = [tour[0] for tour in list_obj if getattr(tour[0], 'NAME') == annex_field][0]
    else:
        # Annex modality must be created
        obj = mat[annex_type]["create_func"](session, annex_field)
        created = True
    trail_attribut.append(obj)
    setattr(new_trail, mat[annex_type]["trail_attribut"], trail_attribut)
    if created:
        # update list
        list_obj.append([obj, ])
        list_mod.append(annex_field)
    return created, obj, list_obj, list_mod


def load_TRAIL(data_file: str, db_maria_connect: dict, db_mongo_connect: dict, db_neo4j_connect: dict, DEBUG: bool):
    # ####################  MariaDB
    session = connect_maria(db_maria_connect=db_maria_connect, DEBUG=DEBUG)

    # search for existing modality Objects into Annex Tables:
    l_tour_type, l_trail_type, l_theme, l_audience = retrieve_annex_modality(
        session=session)
    # tranform into list of modality
    exist_Tour = [Tour[0].NAME for Tour in l_tour_type]
    exist_Type = [Type[0].NAME for Type in l_trail_type]
    exist_Theme = [Theme[0].NAME for Theme in l_theme]
    exist_audience = [Audience[0].NAME for Audience in l_audience]

    l_trails = retrieve_all_trails(session=session)
    print(f"nb trails already in base = {len(l_trails)}")
    l_DataTourism_ID = [trail[0].DataTourism_ID for trail in l_trails]

    # load data from file to variable
    with open(data_file) as f:
        data = json.load(f)

    mongoJSON = []
    trail_list = []

    for POI in data['@graph']:
        try:
            if POI['@id'] in l_DataTourism_ID:
                old_trail = [trail[0] for trail in l_trails
                             if trail[0].DataTourism_ID == POI['@id']][0]
                update_trail(old_trail, POI)
            else:
                new_trail = create_trail(POI)
                # we keep only Type with "Tour" into its name
                Tour_types = [x for x in POI['@type']
                              if re.search('.*Tour$', x)]

                # check if tour_type already exist in annex
                for t_type in Tour_types:
                    _, _, l_tour_type, exist_Tour = append_trail(
                        session=session,
                        annex_type="tourType",
                        annex_field=t_type,
                        list_mod=exist_Tour,
                        list_obj=l_tour_type,
                        new_trail=new_trail,
                        DEBUG=DEBUG
                    )

                # check if trail_type already exist in annex
                try:
                    POI_hasTourType = POI['hasTourType']['@id'][3:]
                    if DEBUG:
                        print(f"trail_types= {POI_hasTourType}")
                    _, _, l_trail_type, exist_Type = append_trail(
                        session=session,
                        annex_type="trailType",
                        annex_field=POI_hasTourType,
                        list_mod=exist_Type,
                        list_obj=l_trail_type,
                        new_trail=new_trail,
                        DEBUG=DEBUG
                    )
                except KeyError:
                    if DEBUG:
                        print("pas de trail_type pour ce POI")
                except TypeError:
                    # it's probably that hasTourType is a list
                    if isinstance(POI['hasTourType'], list):
                        for tourType in POI['hasTourType']:
                            tourType = tourType['@id'][3:]
                            _, _, l_trail_type, exist_Type = append_trail(
                                session=session,
                                annex_type="trailType",
                                annex_field=tourType,
                                list_mod=exist_Type,
                                list_obj=l_trail_type,
                                new_trail=new_trail,
                                DEBUG=DEBUG
                            )

                # check if Theme already exist in annex
                try:
                    theme = POI['hasTheme']["@id"][3:]
                    if DEBUG:
                        print(f"Theme= {theme}")
                    _, _, l_theme, exist_Theme = append_trail(
                        session=session,
                        annex_type="theme",
                        annex_field=theme,
                        list_mod=exist_Theme,
                        list_obj=l_theme,
                        new_trail=new_trail,
                        DEBUG=DEBUG
                    )
                except TypeError:
                    # it's probably that POI_hasTheme is a list
                    if isinstance(POI['hasTheme'], list):
                        for theme in POI['hasTheme']:
                            _, _, l_theme, exist_Theme = append_trail(
                                session=session,
                                annex_type="theme",
                                annex_field=theme["@id"][3:],
                                list_mod=exist_Theme,
                                list_obj=l_theme,
                                new_trail=new_trail,
                                DEBUG=DEBUG
                            )
                except KeyError:
                    if DEBUG:
                        print("pas de THEME pour ce POI")

                # check if Audience already exist in annex
                try:
                    POI_isDedicatedTo = POI['owl:topObjectProperty']['isDedicatedTo']
                    if DEBUG:
                        print(f"Audience= {POI_isDedicatedTo}")
                    try:
                        audience = POI_isDedicatedTo[0]["@id"][3:]
                        _, _, l_audience, exist_audience = append_trail(
                            session=session,
                            annex_type="audience",
                            annex_field=audience,
                            list_mod=exist_audience,
                            list_obj=l_audience,
                            new_trail=new_trail,
                            DEBUG=DEBUG
                        )
                    except TypeError:
                        # it's probably that POI_isDedicatedTo is a list
                        if isinstance(POI_isDedicatedTo, list):
                            for audience in POI_isDedicatedTo:
                                _, _, l_audience, exist_audience = append_trail(
                                    session=session,
                                    annex_type="audience",
                                    annex_field=audience["@id"][3:],
                                    list_mod=exist_audience,
                                    list_obj=l_audience,
                                    new_trail=new_trail,
                                    DEBUG=DEBUG
                                )
                        else:
                            if DEBUG:
                                print(f"pas d'audience pour ce POI {new_trail}")
                except KeyError:
                    if DEBUG:
                        print(f"pas d'audience pour ce POI {new_trail}")

                loc_rep = []
                try:
                    rep = POI['hasRepresentation']
                    if isinstance(rep, list):
                        for dic in rep:
                            try:
                                loc_rep = dic['ebucore:hasRelatedResource']['ebucore:locator']['@value']
                            except KeyError:
                                pass
                            if loc_rep:
                                trailViz = create_trailViz(session=session,
                                                           viz=loc_rep)
                                if trailViz:
                                    new_trail.TRAIL_VISUALIZATION.append(trailViz)
                    else:
                        try:
                            loc_rep = POI['hasRepresentation']['ebucore:hasRelatedResource']['ebucore:locator']['@value']
                        except KeyError:
                            pass
                        if loc_rep:
                            trailViz = create_trailViz(session=session,
                                                       viz=loc_rep)
                            new_trail.TRAIL_VISUALIZATION.append(trailViz)
                except KeyError:
                    pass

                try:
                    # creation of row into DB MariaDB from object
                    session.add(new_trail)
                except exc.IntegrityError as e:
                    session.rollback()
                    print(f"Integrity error on Trail {new_trail} creation\n")
                    print(f"error was {e}\n")
                except ObjectDeletedError:
                    session.rollback()
                    print(f"issue with trail {new_trail}\n")
                except exc.InvalidRequestError:
                    session.rollback()
                    print(f"InvalidRequest error on Trail {new_trail} creation\n")

                trail_list.append(new_trail)

                # Prepare Trail Mongo Data
                mongo_trail = retrieve_mongo_data_POI(POI=POI)

                mongoJSON.append({"_id": new_trail.id,
                                  "LABEL": mongo_trail['LABEL'],
                                  "SHORT_DESCRIPTION": mongo_trail['SHORT_DESCRIPTION'],
                                  "DESCRIPTION": mongo_trail['DESCRIPTION']})

                ###########################################################
                # for NEO4J :
                create_POI_into_neo4j(POI=POI, UUID_gen=new_trail.id, db_neo4j_connect=db_neo4j_connect, node_label="TRAIL")

        except KeyError:
            print(f"pb avec le POI: {POI}")

    try:
        session.commit()
        print(f"{len(trail_list)} trails have been added into DB TRAIL")
    except exc.IntegrityError as e:
        session.rollback()
        print(f"error was {e}\n")
    except ObjectDeletedError as e:
        session.rollback()
        print(f"error was {e}\n")
    except exc.InvalidRequestError as e:
        session.rollback()
        print(f"error was {e}\n")

    session.close()

    # ############### MongoDB #####################
    load_POI_into_mongodb(mongoJSON, db_mongo_connect)


if __name__ == '__main__':

    data_file = DATAFILES_TRAIL
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    DEBUG = False

    # parameter to connect MariaDB (docker Hazem)
    db_maria_connect = {
        'db_host': MARIADB_HOST,
        'db_port': MARIADB_PORT,
        'db_user': MARIADB_USER,
        'db_password': MARIADB_PWD,
        'db_name': MARIADB_DB
    }

    # parameter to connect MongoDB (docker Hazem)
    db_mongo_connect = {
        'uri': MONGODB_URI, 
        'database': MONGODB_DB,
        'collection': MONGODB_TRAIL_COLLECTION
    }

    # parameters to neo4j
    db_neo4j_connect = {
        'uri': NEO4J_URI,
        'username': NEO4J_USER,
        'password': NEO4J_PWD
    }

    load_TRAIL(
        data_file=data_file,
        db_maria_connect=db_maria_connect,
        db_mongo_connect=db_mongo_connect,
        db_neo4j_connect=db_neo4j_connect,
        DEBUG=DEBUG
    )
