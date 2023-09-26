from decouple import config
import os 

#chdir to parent directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# neo4j settings
NEO4J_URI = config('NEO4J_URI', default="bolt://localhost:7687", cast=str)
NEO4J_USER = config('NEO4J_USER', default="neo4j", cast=str)
NEO4J_PWD = config('NEO4J_PWD', default="neo4jneo4j", cast=str)


# mariaBD settings
MARIADB_HOST = config('MARIADB_HOST', default="localhost", cast=str)
MARIADB_PORT = config('MARIADB_PORT', default="3306", cast=str)
MARIADB_USER = config('MARIADB_USER', default="root", cast=str)
MARIADB_PWD = config('MARIADB_PWD', default="", cast=str)
MARIADB_DB = config('MARIADB_DB', default="exploreit", cast=str)

MARIADB_HOSTING_TABLE = config('MARIADB_HOST', default="hostings", cast=str)
MARIADB_WC_TABLE = config('MARIADB_HOST', default="wcs", cast=str)
MARIADB_RESTAURANT_TABLE = config('MARIADB_HOST', default="restaurants", cast=str)

# mongoDB settings
MONGODB_URI = config('MONGODB_URI', default="mongodb://localhost/", cast=str)
MONGODB_DB = config('MONGODB_DB', default="exploreit", cast=str)

MONGODB_HOSTING_COLLECTION = config('MONGODB_HOSTING_COLLECTION', default="hostings", cast=str)
MONGODB_WC_COLLECTION = config('MONGODB_WC_COLLECTION', default="wcs", cast=str)
MONGODB_RESTAURANT_COLLECTION = config('MONGODB_RESTAURANT_COLLECTION', default="restaurants", cast=str)
MONGODB_POI_COLLECTION = config('MONGODB_POI_COLLECTION', default="pois", cast=str)
MONGODB_TRAIL_COLLECTION = config('MONGODB_TRAIL_COLLECTION', default="pois", cast=str)

# datafiles for POI & TRAIL
DATAFILES_POI = config('DATAFILES_POI', default='../raw_data/datatourisme/POI/objects/', cast=str)
DATAFILES_TRAIL = config('DATAFILES_TRAIL', default='../raw_data/datatourisme/TRAIL/tous_itineraire_NA_structured.jsonld', cast=str)