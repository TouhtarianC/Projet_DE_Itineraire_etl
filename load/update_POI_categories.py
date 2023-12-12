""" Load Datatourisme data and parse it to :
    - create MariaDB tables
    - create documents to load data into MongoDB
    - select data to load into Neo4j
    - set data into MariaDB tables
"""

from sqlalchemy import update
import pandas as pd

from lib.create_DB_ORM import PoiType, PoiTheme
from lib.load_maria import connect_maria

from settings import MARIADB_DB, MARIADB_HOST, \
    MARIADB_PORT, MARIADB_USER, MARIADB_PWD, \
    FILE_CATEGORIES_POI_TYPE, FILE_CATEGORIES_POI_THEME


def update_DATATOURISME_POI_TYPE(
        db_maria_connect: dict,
        file_poi_type: str,
        DEBUG: bool):

    session = connect_maria(db_maria_connect=db_maria_connect, DEBUG=DEBUG)
    
    df = pd.read_csv(file_poi_type, sep=";")
    for index, row in df.iterrows():
        if row.TYPE != '':
            update_statement = update(PoiType).where(PoiType.NAME == row.TYPE).values(CATEGORY=row.TYPE_AGG)
            session.execute(update_statement)
            session.commit()

def update_DATATOURISME_POI_THEME(
        db_maria_connect: dict,
        file_poi_theme: str,
        DEBUG: bool):

    session = connect_maria(db_maria_connect=db_maria_connect, DEBUG=DEBUG)
    
    df = pd.read_csv(file_poi_theme, sep=";")
    for index, row in df.iterrows():
        if row.THEME != '':
            update_statement = update(PoiTheme).where(PoiTheme.NAME == row.THEME).values(CATEGORY=row.THEME_AGG)
            session.execute(update_statement)
            session.commit()



if __name__ == '__main__':

    DEBUG = False

    # parameters to connect MariaDB
    db_maria_connect = {
        'db_host': MARIADB_HOST,
        'db_port': MARIADB_PORT,
        'db_user': MARIADB_USER,
        'db_password': MARIADB_PWD,
        'db_name': MARIADB_DB
    }

    update_DATATOURISME_POI_TYPE(
        db_maria_connect=db_maria_connect,
        file_poi_type=FILE_CATEGORIES_POI_TYPE,
        DEBUG=DEBUG)
    
    
    update_DATATOURISME_POI_THEME(
        db_maria_connect=db_maria_connect,
        file_poi_theme=FILE_CATEGORIES_POI_THEME,
        DEBUG=DEBUG)

    print("End update categories of POI TYPE and POI THEME")
