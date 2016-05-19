#!/bin/bash

wget http://apache.ip-connect.vn.ua/pig/latest/pig-0.15.0.tar.gz
tar -xzf pig-0.15.0.tar.gz
rm pig-0.15.0.tar.gz

wget http://central.maven.org/maven2/com/twitter/parquet-pig-bundle/1.6.0/parquet-pig-bundle-1.6.0.jar
mv parquet-pig-bundle-1.6.0.jar pig-0.15.0/lib/

wget http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip
mkdir datasets/geolitecity/ -p
unzip -j GeoLiteCity-latest.zip -d datasets/geolitecity/
tail -n +3  datasets/geolitecity/GeoLiteCity-Location.csv > datasets/geolitecity/location.csv
rm datasets/geolitecity/GeoLiteCity-Location.csv
rm GeoLiteCity-latest.zip

#RITA - airplanes
#wget http://stat-computing.org/dataexpo/2009/2008.csv.bz2

mkdir out/
