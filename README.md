# EXPOSEIT :building_construction: :bar_chart: :airplane:

this project permits to retrieve data about **Points of Interest**. For the remainder of this document, we will refer to this as **POI**. The POI data will be consumed by an application named Navigo (its repo is [here](https://github.com/DataScientest-Studio/JAN23_ORANGE_Itineraire_navigo) ) 

## 1 -  Sources
This project will collect data from the following sources:
- [Datatourisme](https://www.datatourisme.fr/): 
      - about POIs such as places, museum, church...
      - about POIs such as trails (hiking, horse trail, cycle trail...) (named TRAILS in the rest of this doc)
- [GeoDatamine](https://geodatamine.fr/):
      - about Hotels
      - about Restaurants
      - about toilets

## 2 - Results
Data from these 3 sources will be extracted, transformed and loaded into 3 databases:
 - 1 MariaDB database for tabular data 
 - 1 MongoDB database for "textual" data (description texts, translation dictionnary, etc.)
 - 1 Neo4J database for geographical coordinates data

All the details about the schema of theses databases are described [here](hld_project_de_v6.drawio)

## 3 - Project Structure:
- **root** : 
      - docker-compose.yml => permits the orchestration of containers needed to run this project.
      - hld_project.drawio => gives a high-level description of project, such as BDD schemas
      - requirements.txt => lists and versions of packages needed to run the project.
- **raw_data**: contains an example of data from sources, so you can run this project without downloading fresh data from the sources.
- **collect**: contains scripts to download fresh data from the sources
- **load**: contains scripts to extract/tranform and load data into final databases.

## 4- To execute ETL :
:warning: you need **SPARK 3.4.1** and **Python3.10** to run this project

 - create your virtual env python with **requirements.txt**
 - run containers with **docker-compose.yml** file
 - to retrieve fresh data from the Datatourime source, execute **collect/download_Datatourism_ws.sh** (you must export your API_KEY into environment variables first)
 - execute **load/load_POI.py** to load Datatourime's data about POIs into the 3 BDD
 ```code
 python3.10 load/load_POI.py
 ```
 - execute **load/load_TRAIL.py** to load Datatourime's data about TRAILs into the 3 BDD
 ```code
 python3.10 load/load_TRAIL.py
 ```
 - execute **load/load_GEODATAMINE.py** to load Geodatamine's data about hotels, restaurants and toilets into the 3 BDD
 ```code
cd load/
python3.10 load_GEODATAMINE.py -t wc ../raw_data/geodatamine/nouvelle-aquitaine_toilets_2023-07-04/data.csv 
python3.10 load_GEODATAMINE.py -t restaurant ../raw_data/geodatamine/nouvelle-aquitaine_restaurant_2023-07-04/data.csv 
python3.10 load_GEODATAMINE.py -t hosting ../raw_data/geodatamine/nouvelle-aquitaine_hosting_2023-07-04/data.csv
 ```

 all BDD data will be persisted into a directory named **db_persistence**

 :warning: don't stop BDD containers

:bulb: if you want reload data into BDD, don't forget to restart spark containers before.