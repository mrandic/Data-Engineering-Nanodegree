create_temperature = """
CREATE TABLE IF NOT EXISTS temperature (
    timestamp                      DATE,
    average_temperature            FLOAT,
    average_temperature_uncertainty FLOAT,
    city                           VARCHAR,
    country                        VARCHAR,
    latitude                       VARCHAR,
    longitude                      VARCHAR
);
"""

temperature_insert = ("""
INSERT INTO temperature 
(
timestamp, 
average_temperature, 
average_temperature_uncertainty, 
city, 
country,
latitude, 
longitude
)
VALUES (%s, %s, %s, %s, %s, %s, %s)""")

drop_temperature = "DROP TABLE IF EXISTS temperature;"


create_immigrations = """
CREATE TABLE IF NOT EXISTS immigrations (
    cicid    FLOAT PRIMARY KEY,
    year     FLOAT,
    month    FLOAT,
    iata     VARCHAR(3),
    arrdate  DATE,
    depdate  DATE,
    visa     FLOAT,
    biryear  FLOAT,
    gender   VARCHAR(1),
    airline  VARCHAR,
    fltno    VARCHAR,
    visatype VARCHAR
);
"""

drop_immigrations = "DROP TABLE IF EXISTS immigrations;"

immigration_insert = ("""
INSERT INTO immigrations 
(
cicid,
year,
month,
iata,
arrdate,
depdate,
visa,
biryear,
gender,
airline,
fltno,
visatype
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")


create_demographics = """
CREATE TABLE IF NOT EXISTS demographics (
    city                   VARCHAR,
    state                  VARCHAR,
    median_age             FLOAT,
    male_population        FLOAT,
    female_population      FLOAT,
    total_population       FLOAT,
    num_veterans           FLOAT,
    foreign_born           FLOAT,
    avg_household_size     FLOAT,
    state_code             VARCHAR(2),
    race                   VARCHAR,
    count                  INT
);
"""

drop_demographics = "DROP TABLE IF EXISTS demographics;"

demographic_insert = """
INSERT INTO demographics 
(
city, 
state, 
median_age, 
male_population, 
female_population, 
total_population,
num_veterans, 
foreign_born, 
avg_household_size, 
state_code, 
race, 
count
) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


create_airports = """
CREATE TABLE IF NOT EXISTS airports (
    iata_code    VARCHAR PRIMARY KEY,
    name         VARCHAR,
    type         VARCHAR,
    local_code   VARCHAR,
    coordinates  VARCHAR,
    city         VARCHAR,
    elevation_ft FLOAT,
    continent    VARCHAR,
    iso_country  VARCHAR,
    iso_region   VARCHAR,
    municipality VARCHAR,
    gps_code     VARCHAR
);
"""

drop_airports = "DROP TABLE IF EXISTS airports;"

airport_insert = """
INSERT INTO airports 
(
iata_code, 
name, 
type, 
local_code, 
coordinates, 
city, 
elevation_ft, 
continent,
iso_country, 
iso_region, 
municipality, 
gps_code
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

drop_table_queries   = [drop_airports, drop_demographics, drop_immigrations, drop_temperature]
create_table_queries = [create_airports, create_demographics, create_immigrations, create_temperature]