#!/bin/bash

# to download data file from Datatoursim webservice
# TRAIL_API_KEY & POI_API_KEY must have been export into environment variables

URL_Trail="https://diffuseur.datatourisme.fr/webservice/2e8397ae2f5cf4f005439e7442286231/$TRAIL_API_KEY"
URL_POI="https://diffuseur.datatourisme.fr/webservice/78c74334eb2e3c59690fe54b2005ef1e/$POI_API_KEY"

ma_date_heure=$(date +"%Y%m%d_%H%M%S")

cd ../raw_data/datatourisme/TRAIL
curl --compressed $URL_Trail -o tous_itineraire_NA_structured_$ma_date_heure.json
# on ecrase l'ancien fichier
cp -p tous_itineraire_NA_structured_$ma_date_heure.json tous_itineraire_NA_structured.jsonld

cd ../POI/
curl --compressed $URL_POI -o POI_$ma_date_heure.zip
unzip -of POI_$ma_date_heure.zip