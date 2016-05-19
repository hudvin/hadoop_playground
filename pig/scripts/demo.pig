rmf out/
locations = LOAD 'datasets/geolitecity/location.csv' USING PigStorage(',') AS (locId, country, region, city, postalCode, latitude, longitude, metroCode, areaCode);
STORE locations INTO 'out/parquet_locations' USING ParquetStorer;

grouped_by_country = GROUP locations BY country;
cities_per_country = FOREACH grouped_by_country GENERATE group, COUNT(locations.city);
city_in_country = FOREACH locations GENERATE CONCAT(CONCAT(city,' in ') , country);
STORE city_in_country INTO 'out/csv_city_in_country/' USING PigStorage(',');

STORE cities_per_country INTO 'out/json_cities_per_country' USING JsonStorage(); 
STORE grouped_by_country INTO 'out/avro_cities_per_country' using AvroStorage();

