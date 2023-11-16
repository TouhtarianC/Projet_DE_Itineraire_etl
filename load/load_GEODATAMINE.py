import argparse
import csv
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from neo4j import GraphDatabase
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import pymongo

from settings import NEO4J_URI, NEO4J_USER, NEO4J_PWD, MARIADB_HOSTING_TABLE, \
    MARIADB_HOST, MARIADB_USER, MARIADB_PORT, MARIADB_PWD, MARIADB_DB, \
    MARIADB_WC_TABLE, MARIADB_RESTAURANT_TABLE, MONGODB_URI, MONGODB_DB, \
    MONGODB_WC_COLLECTION, MONGODB_RESTAURANT_COLLECTION, \
    MONGODB_HOSTING_COLLECTION


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest CSV data files and store data in databases")
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("-t", "--type",
                        choices=["wc", "restaurant", "hosting"],
                        help="Type of nodes to ingest: \
                            WC, Restaurant, or Hosting")
    return parser.parse_args()


def main():
    args = parse_args()
    csv_file_path = args.csv_file
    node_type = args.type

    print(f"Loading {node_type} data to databases from {csv_file_path}")

    # Load the CSV file into a PySpark DataFrame
    spark = SparkSession.builder \
        .appName("Load Data to DB") \
        .master("local[4]") \
        .getOrCreate()

    # load insee code to 'la poste' postal code mapping
    insee_postal_code_mapping = {}
    with open("../raw_data/geodatamine/laposte-insee.csv", 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            # skip Région Corse(94) (as their insee code is not an integer)
            try:
                insee_postal_code_mapping.update(
                    {int(row['#Code_commune_INSEE']): int(row['Code_postal'])}
                )
            except ValueError:
                continue

    print(f'loaded {len(insee_postal_code_mapping)} INSEE code to postal code mapping ..')

    # load geodatamine file (passed as script argument)
    df = spark.read.option("header", "true").option(
        "delimiter", ";").csv(csv_file_path)

    # Filter the lines where specified columns are null
    required_columns = ["X", "Y", "osm_id",
                        "type", "name", "com_insee", "com_nom"]
    df = df.na.drop(subset=required_columns)

    # Rename columns X to longitude, Y to latitude,
    # com_insee to POSTAL_CODE and com_nom to CITY
    df = df \
        .withColumnRenamed("X", "LONGITUDE") \
        .withColumnRenamed("Y", "LATITUDE") \
        .withColumnRenamed("com_insee", "POSTAL_CODE") \
        .withColumnRenamed("com_nom", "CITY") \
        .withColumnRenamed("name", "NAME") \
        .withColumnRenamed("type", "TYPE") \
        .withColumnRenamed("osm_id", "OSM_ID")

    # change all POSTAL_CODE values from INSEE to POSTAL CODE
    @F.udf(StringType())
    def insee_to_postal_code(x):
        try:
            postal_code = insee_postal_code_mapping[int(x)]
        except KeyError:
            # unknown insee code
            postal_code = int(x)
        return str(postal_code)

    df = df.withColumn('POSTAL_CODE', insee_to_postal_code(df.POSTAL_CODE))

    # Generate a new UUID column for each row
    @F.udf(StringType())
    def generate_node_uuid():
        return str(uuid.uuid4())
    
    # spark.udf.register('generate_node_uuid', generate_node_uuid)
    df = df.withColumn('UUID', (generate_node_uuid()))

    # clean hosting names
    @F.udf(StringType())
    def clean_node_name(n):
        return n.strip('"')

    # spark.udf.register('clean_node_name', clean_node_name)
    df = df.withColumn('NAME', clean_node_name(df.NAME))

    print("finished cleaning and transforming data.. dataframe schema:")
    df.printSchema()
    print(df.head(10))

    ##################################################################
    print("loading data to neo4j ..")

    # Connect to neo4j and create a node
    # for each element of the DataFrame
    mongo_data_list = df.rdd.map(lambda row: row.asDict()).collect()

    # Prepare Cypher query for bulk insertion
    cypher_query = f"""
    UNWIND $dataList as data
    MERGE (r:{node_type} {{
    LONGITUDE: data.LONGITUDE,
    LATITUDE: data.LATITUDE,
    UUID: data.UUID
    }})
    """

    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PWD)) as driver:
        with driver.session() as session:
            # Perform bulk insert using a single Cypher query
            session.run(cypher_query, dataList=mongo_data_list)

    ##################################################################
    print("loading data to mongodb ..")

    # Connect to MongoDB and create a document
    # for each element of the DataFrame
    def create_mongodb_document(record):
        document = record.asDict()
        del document["OSM_ID"]
        del document["TYPE"]
        del document["NAME"]
        del document["POSTAL_CODE"]
        del document["CITY"]

        return document

    # transform df to rdd
    mongo_docs_rdd = df.rdd.map(lambda x: create_mongodb_document(x))
    mongo_doc_list = mongo_docs_rdd.collect()

    mongo_client = pymongo.MongoClient(MONGODB_URI)
    mongo_db = mongo_client[MONGODB_DB]
    mongo_collection = None
    match node_type:
        case "hosting":
            mongo_collection = mongo_db[MONGODB_HOSTING_COLLECTION]
        case "restaurant":
            mongo_collection = mongo_db[MONGODB_RESTAURANT_COLLECTION]
        case "wc":
            mongo_collection = mongo_db[MONGODB_WC_COLLECTION]

    mongo_collection.insert_many(mongo_doc_list)

    ##################################################################
    print("loading data to mariadb ..")

    # Connect to MariaDB and create a new row 
    # for each element of the DataFrame
    SQLALCHEMY_DATABASE_URI = \
        f'mysql+pymysql://{MARIADB_USER}:{MARIADB_PWD}@{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}'
    engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=False)
    if not database_exists(engine.url):
        create_database(engine.url)

    table_name = ""
    match node_type:
        case "hosting":
            table_name = MARIADB_HOSTING_TABLE
        case "wc":
            table_name = MARIADB_WC_TABLE
        case "restaurant":
            table_name = MARIADB_RESTAURANT_TABLE

    with engine.begin() as con:
        query = text(
            f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                UUID VARCHAR(50) NOT NULL,
                OSM_ID VARCHAR(50) NOT NULL,
                TYPE VARCHAR(50) NOT NULL,
                NAME VARCHAR(250) NOT NULL,
                POSTAL_CODE INT NOT NULL,
                CITY VARCHAR(50) NOT NULL,
                PRIMARY KEY (UUID)
                )
            """
        )
        con.execute(query)

    # todo more functional
    def insert_into_mariadb(record):
        return f"""
            INSERT INTO {table_name} (UUID, OSM_ID, TYPE, NAME, POSTAL_CODE, CITY)
            VALUES ('{record.UUID}', '{record.OSM_ID}', '{record.TYPE}', "{record.NAME}", {record.POSTAL_CODE}, "{record.CITY}");
            """

    sql_query_rdd = df.rdd.map(lambda x: insert_into_mariadb(x))
    sql_query_list = sql_query_rdd.collect()

    # todo: optimize me ?
    with engine.begin() as con:
        for query in sql_query_list:
            con.execute(text(query))

    # Stop the Spark session
    spark.stop()

    print(f"successfully loaded {node_type} data to databases")


if __name__ == "__main__":
    main()
