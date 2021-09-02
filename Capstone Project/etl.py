# Do all imports and installs here
import pandas as pd
import psycopg2
import datetime
from sql_queries import airport_insert, demographic_insert, immigration_insert, temperature_insert



def transform_sas_date(day_cnt):
    """
    Transforms SAS date stored as days since 1/1/1960 to datetime type
    :param day_cnt: Number of days since 1/1/1960
    :return: datetime
    """
    if day_cnt is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days = day_cnt)


def process_port_locations_data(file_path):
    """
    Perform cleaning and filtering process for I94_SAS_Labels_Descriptions data
    :param file_path: path to file for reading
    :return: df_port_locations for data model
    """
    
        # Read port locations from SAS file
    with open(file_path) as i94f:
        i94_sas_label_desc = i94f.readlines()

    i94_sas_label_desc = [x.strip() for x in i94_sas_label_desc]
    ports_unformatted  = i94_sas_label_desc[302:962]
    ports_formatted    = [x.split("=") for x in ports_unformatted]

    # extract port codes from string array
    port_codes = [x[0].replace("'","").strip() for x in ports_formatted]

    # extract location in city-state format
    port_locations = [x[1].replace("'","").strip() for x in ports_formatted]
    # extract city as first element from splited string array
    port_cities = [x.split(",")[0] for x in port_locations]
    # extract state as last element from splited string array
    port_states = [x.split(",")[-1] for x in port_locations]

    # define port locations data frame
    df_port_locations = pd.DataFrame({"port_code": port_codes, "port_city": port_cities, "port_state": port_states})
    
    return df_port_locations


def process_temperature_data(df_temperature):
    """
    Perform cleaning and filtering process for temperature data
    :param df_temperature: Raw data frame to be processed
    :return: df_temperature for data model
    """
    # Filter temperature data for US only
    df_temperature = df_temperature[df_temperature["Country"] == "United States"]
    # clear records with missing values
    # df_temperature.drop(columns=["Unnamed: 0"], inplace=True)
    df_temperature.dropna(inplace=True)

    return df_temperature


def process_i94_data(df_i94):
    """
    Perform cleaning and filtering process for df_i94 data
    :param df_i94: Raw data frame to be processed
    :return: df_i94
    """
    # exclude columns not needed for further processing
    df_i94.drop(columns=["insnum", "entdepu", "occup", "visapost"], inplace=True)
    # clear records with missing values
    df_i94.dropna(inplace=True)
    # process SAS date into readable date format
    df_i94["arrdate"] = [transform_sas_date(x) for x in df_i94["arrdate"]]
    df_i94["depdate"] = [transform_sas_date(x) for x in df_i94["depdate"]]

    df_i94 = df_i94[df_i94.depdate >= df_i94.arrdate]
    # variable selection for data model
    df_i94 = df_i94[
        ["cicid", "i94yr", "i94mon", "i94port", "arrdate", "depdate", "i94visa", "biryear", "gender", "airline",
         "fltno", "visatype"]]

    return df_i94



def process_airport_codes_data(df_airport_codes, df_port_locations):
    """
    Perform cleaning and filtering process for airport_codes data
    :param airport_codes: Raw data frame to be processed
    :return: airport_codes
    """
    # clear records with missing values
    df_airport_codes.dropna(inplace=True)
    # merge datasets
    df_airport_codes = df_airport_codes.merge(df_port_locations, left_on="iata_code", right_on="port_code")
    df_airport_codes.drop(columns=["port_code"], inplace=True)
    # remove duplicated rows
    df_airport_codes = df_airport_codes.drop_duplicates()
    # define set of columns for final dataset
    df_airport_codes = df_airport_codes[["iata_code", "name", "type", "local_code", "coordinates", "port_city", "elevation_ft", "continent", "iso_country", "iso_region", "municipality", "gps_code"]]
    
    return df_airport_codes


def connect_to_postgres():
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacitydb user=student password=student")
    cur = conn.cursor()
    return conn, cur


def insert_data(sql_statement, dframe):
    """
    Executes SQL insert statement on postgresql
    :param conn: active connection
    :param curr: cursor
    :param sql_statement: sql statement to be executed
    :param dframe: data to be inserted
    """
    conn, cur = connect_to_postgres()
    try:
        for index, row in dframe.iterrows():
            cur.execute(sql_statement, list(row.values))
        conn.commit()
        print("Data insert was successfull")
    except:
        print("Data insert failed")
    finally:
        conn.close()
        
        

def data_volume_check(table_name):
    """
    Checks how many rows were inserted in a table.
    """
    conn, cur = connect_to_postgres()
    try:
        sql_statement = "SELECT * FROM " + table_name
        cur.execute(sql_statement)
        conn.commit()
        if cur.rowcount < 1:
            print(table_name, "table contains no data")
        else:
            print(table_name, "table contains rows: ", cur.rowcount)
    except:
        print("Data check failed for table:", table_name)
    finally:
        conn.close()


def valid_date_immigrations_check():
    """
    Add constraint check on arrdate and depdate logic.
    """
    conn, cur = connect_to_postgres()
    try:
        sql_statement = """ ALTER TABLE immigrations 
                            ADD CONSTRAINT valid_range_check 
                            CHECK (depdate >= arrdate)"""
        cur.execute(sql_statement)
        conn.commit()
        print("Valid date check success for table immigrations.")
    except:
        print("Valid date check failed for table immigrations.")
    finally:
        conn.close()

def extract_example_data():
    """
    Extract example data from database and join two tables
    """
    conn, cur = connect_to_postgres()
    try:
        df = pd.read_sql_query("SELECT a.*, b.* FROM immigrations a left join airports b on a.iata=b.iata_code", con=conn)
        return df
    except:
        print("Data read failed.")
    finally:
        conn.close()
    
def main():
    """
    Main function, used for executing etl flow.
    It connects to database and processes imported files.
    
    """
    # Read input datasets
    df_i94            = pd.read_sas('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', 'sas7bdat', encoding="ISO-8859-1")
    df_demographics   = pd.read_csv("./us-cities-demographics.csv", delimiter=";")
    df_temperature    = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
    df_airport_codes  = pd.read_csv("./airport-codes_csv.csv")
    
    # Process datasets and prepare tables for data model
    df_port_locations = process_port_locations_data("./I94_SAS_Labels_Descriptions.SAS")
    df_temperature    = process_temperature_data(df_temperature)
    df_i94            = process_i94_data(df_i94)
    df_airport_codes  = process_airport_codes_data(df_airport_codes, df_port_locations)

    # Insert data into postgres database
    insert_data(airport_insert, df_airport_codes)
    insert_data(demographic_insert, df_demographics)
    insert_data(immigration_insert, df_i94)
    insert_data(temperature_insert, df_temperature)
    
    # Run quality checks
    data_volume_check("immigrations")
    data_volume_check("demographics")
    data_volume_check("temperature")
    data_volume_check("airports")
    
    valid_date_immigrations_check()

if __name__ == "__main__":
    main()