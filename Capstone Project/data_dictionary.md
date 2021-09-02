# Data Dictionary

## temperature

|Field|Type|Description|
|----|-----|-----------|
|timestamp|date|Timestamp of measurement|
|average_temperature|float|Avg temperaure|
|average_temperature_uncertainty|float|Avg. uncertainty temperature|
|city|varchar|City name (foreign key to demographics.city)|
|country|varchar|Country name (foreign key to demographics.state)|
|latitude|varchar|Latitude measure|
|longitude|varchar|Longitude measure|


## demographics

|Field|Type|Description|
|----|-----|-----------|
|city|varchar|City name|
|state|varchar|State name|
|median_age|float|The median age|
|male_population|float|Count of male population|
|female_population|float|Count of female population|
|total_population|float|Count of population|
|num_veterans|float|Count of veterans|
|foreign_born|float|Count of foreign born people|
|avg_household_size|float|Average household size|
|state_code|varchar(2)|State code|
|race|varchar|Race name|
|count|int|Count for this demographic|

## airports

|Field|Type|Description|
|----|-----|-----------|
|iata_code|varchar|Primary Key|
|name|varchar|Airport name|
|type|varchar|Description of airport type|
|local_code|varchar|Local code|
|coordinates|varchar|Airport Coordinates|
|city|varchar|Airport city|
|elevation_ft|float|Airport elevation in ft|
|continent|varchar|Airport Continent|
|iso_country|varchar|Airport country (foreign key to demographics.state_code)|
|iso_region|varchar|Airport region|
|municipality|varchar|Airport municipality|
|gps_code|varchar|Airport GPS code|

## immigration

|Field|Type|Description|
|----|-----|-----------|
|cicid|float|Primary Key|
|year|float|Year of event|
|month|float|Month of event|
|iata|varchar(3)|Port number (foreign key to airports.iata_code)|
|arrdate|date|Arrival date|
|depdate|date|Departure date|
|visa|float|VISA category|
|biryear|float|Year of birth of immigrant|
|gender|varchar(1)|Immigrant gender|
|airline|varchar|Airline name|
|fltno|varchar|Fleet number|
|visatype|varchar|Type of VISA|