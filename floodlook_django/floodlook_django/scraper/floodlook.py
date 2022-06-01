import xml.etree.ElementTree as ET
import urllib.request
import sys
import time
import config
import psycopg2
from psycopg2 import Error

start_time = time.time()
verbose = False

##This is just a test of the DB connection
##Not used in the script

def test_external_connect():
    try:
    # Connect to an existing database
        pgconnection = psycopg2.connect(user= config.user,
                                    password= config.password,
                                    host= config.host,
                                    port= config.port,
                                    database= config.database)
        # Create a cursor to perform database operations
        cursor = pgconnection.cursor()
        # Print PostgreSQL details
        print("PostgreSQL server information")
        print(pgconnection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
#    finally:
#        if (pgconnection):
#            cursor.close()
#            pgconnection.close()
#            print("PostgreSQL connection is closed")


## Inserts records into DB after they've been deduped
## Takes in a list of tuples for values, str variable of which table and the connection
def pinsertIntoTable(tuple_list, table, pgconnection):
    try:

        cursor = pgconnection.cursor()
        pg_insert_query = None

        forecast_insert_query = """
        INSERT INTO floodlook_looker_forecast
            (forecast_time_added,
            forecast_time,
            forecast_stage,
            forecast_flow,
            forecast_gauge_id_id)
            VALUES (%s, %s, %s, %s, (select gauge_pk from floodlook_looker_gauges where gauge_id = %s));
        """

        obs_insert_query = """
        INSERT INTO floodlook_looker_observations
            (observation_time,
            observation_stage,
            observation_flow,
            observation_gauge_id_id)
            VALUES (%s, %s, %s, (select gauge_pk from floodlook_looker_gauges where gauge_id = %s));
            """

        if table == "observations":
            pg_insert_query = obs_insert_query
        elif table == "forecasts":
            pg_insert_query = forecast_insert_query
        else:
            print("Error: didn't find table query in insertIntoTable")
            sys.exit()
        cursor.executemany(pg_insert_query, tuple_list)
        pgconnection.commit()
        print("Query executed successfully")
#        logtuple = (table, len(tuple_list),)
        cursor.close()
    except (Exception, Error) as error:
        print("Failed to insert Python variable into pg table", error)
#    finally:
#        if connection:
#            connection.close()
#            print("The PG connection is closed")





## gets the gauges from DB, returns as a tuple of
## gauge_id in matching value to the NOAA ID
def get_pgauges(pgconnection):
    try:
        cursor = pgconnection.cursor()
        postgreSQL_select_Query = "select gauge_id from floodlook_looker_gauges"

        cursor.execute(postgreSQL_select_Query)
        print("Grabbing the gaugelist")
        gauge_list = cursor.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if pgconnection:
            cursor.close()
#            connection.close()
#            print("PostgreSQL connection is closed")
        return gauge_list


# Pulls the data from NOAA site, returns as ET object

def getonlinedata(which_gauge):
    global tree, root
    print("Executing getdata")
    url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage="+ which_gauge +"&output=xml"
    response = urllib.request.urlopen(url)
    tree = ET.parse(response)
    root = tree.getroot()
    guage_id = root.attrib.get("id")
    gen_time = root.attrib.get("generationtime")
    return tree



## queries the database for all stored observations with the relevant gauge
## update this later to just to the last X observations to it doesn't pull a bajillion
## but idk I'll do that later

def pget_stored_observations(pgconnection, which_gauge):
    try:
#        pgconnection = psycopg2.connect(user= config.user,
#                                    password= config.password,
#                                    host= config.host,
#                                    port= config.port,
#                                    database= config.database)
        cursor = pgconnection.cursor()

        oinsert_query = ("""
            SELECT
                floodlook_looker_observations.observation_time,
                floodlook_looker_observations.observation_stage,
                floodlook_looker_observations.observation_flow,
                floodlook_looker_gauges.gauge_id
            FROM
                floodlook_looker_observations INNER JOIN floodlook_looker_gauges
            ON
                floodlook_looker_observations.observation_gauge_id_id = floodlook_looker_gauges.gauge_pk
            WHERE
                floodlook_looker_gauges.gauge_id = %s
                """)

        cursor.execute(oinsert_query , (which_gauge,))
        stored_observations = cursor.fetchall()
        print("You've got "+ str(len(stored_observations))+ " observations stored.")
        cursor.close()
    except (Exception, Error) as error:
        print("Failed to insert Python variable into pg table", error)
#    finally:
#        if pgconnection:
#            pgconnection.close()
#            print("The PG connection is closed")
    return stored_observations




### Queries the PG DB to get the stored forecasts for the relevant gauge
### Later I should change this to jsut do the last X records when I feel like it
def pget_stored_forecasts(pgconnection, which_gauge):
    try:
        cursor = pgconnection.cursor()

        finsert_query = ("""
            SELECT
                floodlook_looker_forecast.forecast_time_added,
                floodlook_looker_forecast.forecast_time,
                floodlook_looker_forecast.forecast_stage,
                floodlook_looker_forecast.forecast_flow,
                floodlook_looker_gauges.gauge_id
            FROM
                floodlook_looker_forecast INNER JOIN floodlook_looker_gauges
            on
            floodlook_looker_forecast.forecast_gauge_id_id = gauge_pk
            WHERE
            floodlook_looker_gauges.gauge_id = %s
                """)

        cursor.execute(finsert_query,(which_gauge,))
        stored_forecasts = cursor.fetchall()
        print("You've got "+ str(len(stored_forecasts))+ " forecasts stored.")
    except (Exception, Error) as error:
        print("Failed to insert Python variable into pg table", error)
#    finally:
#        if pgconnection:
#            pgconnection.close()
#            print("The PG connection is closed")
    return stored_forecasts


#Parses the ET Object returned by getonlinedata()
#Pulls observations, returns as list of tuples

def parseobservations(tree):
    obs_list = []
    print("+++++Parsing Observations ++++++")
    for elem in tree.iterfind("observed/datum"):
        stage = None
        datetime = None
        flow = None
        obs_gauge_id_value = root.attrib.get("id")
        for child in elem:
            if str(child.attrib.get("name")) == "Stage":
                stage = float(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if str(child.attrib.get("name")) == "Flow":
                flow = float(child.text)
            obs_entry_list = (datetime, stage, flow, obs_gauge_id_value)
        obs_list.append(obs_entry_list)
    print("there's " + str(len(obs_list))+ " observations in the xml")
    return obs_list

# Parses the ET object from getonlinedata() 
# parses for  forecasts and returns as list of tuples
def parseforecast(tree):
    print("+++++Parsing Forecasts+++++")
    forecast_list = []
    forec_issue_time = tree.find("forecast").attrib.get("issued")
    forec_gauge_id_value = root.attrib.get("id")
    for elem in tree.iterfind("forecast/datum"):
        stage = "None"
        datetime = "None"
        flow = None
        for child in elem:
            if child.attrib.get("name") == "Stage":
                stage = float(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if child.attrib.get("name") == "Flow":
                flow = float(child.text)
        forec_entry_list = (forec_issue_time, datetime, stage, flow, forec_gauge_id_value)
        forecast_list.append(forec_entry_list)

    return forecast_list



# Takes results from parseforecast or parseobsercations as list1
# compares with results from the database as list2
# removes anthing in list1 that's already in list2
# so we don't write a bunch of dupes in there

def remove_duplicates(list1, list2):
    for element in list1:
        if element in list2:
            list2.remove(element)
    return list2



# this is kinda like the main and ties most of the other crap
# together but inside the for loop from main

def pull_and_write_main(which_gauge,pgconnection):
    print("_____________________________")
    print("running pull and write for "+ which_gauge)
    tree = getonlinedata(which_gauge)

    stored_observations = pget_stored_observations(pgconnection, which_gauge)

   # print("we've got "+str(len(stored_observations))+ " stored observations in the DB")
    forecast_list = parseforecast(tree)
    print("we've got "+ str(len(forecast_list))+ " forecasts entries that we found online")
    observation_list =   parseobservations(tree)
    stored_forecasts = pget_stored_forecasts(pgconnection, which_gauge)
    print("__________observation list____________")
    if verbose == True:
        print("************Here's the fresh observations from the internet************")
        for item in observation_list:
            print(item)
        print("*************Here's all the forecasts we have stored**********")
        for item in stored_forecasts:
            print(item)
    if stored_forecasts:
        deduped_forec_list = remove_duplicates(stored_forecasts, forecast_list)
    else:
        deduped_forec_list = forecast_list
    print("we have "+ str(len(deduped_forec_list))+ " fresh forecasts to add")
    if stored_observations:
        deduped_obs_list = remove_duplicates(stored_observations, observation_list)
    else:
        deduped_obs_list = observation_list
    print("we have "+ str(len(deduped_obs_list))+ " fresh observations to add")
    if len(deduped_obs_list)>0:
        pinsertIntoTable(deduped_obs_list, "observations",pgconnection)
    if len(deduped_forec_list)>0:
        pinsertIntoTable(deduped_forec_list, "forecasts",pgconnection)
    print("_____________________________")



def main():

    pgconnection = psycopg2.connect(user= config.user,
                                password= config.password,
                                host= config.host,
                                port= config.port,
                                database= config.database)
    gauge_list = get_pgauges(pgconnection)
    print(gauge_list)
    for gauge in gauge_list:
        pull_and_write_main(gauge[0],pgconnection)
    if (pgconnection):
#        cursor.close()
        pgconnection.close()
        print("PostgreSQL connection is closed")


#Tests connection to external postgres DB, prints DB info

#main()
# test_external_connect()
# print(get_pgauges())



print( "This whooooole thing took ", time.time() - start_time, "to run")
