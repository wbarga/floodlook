import xml.etree.ElementTree as ET
import urllib.request
import xmltodict
import sys
import sqlite3
import time
start_time = time.time()


#Setting the global variables
flood_db = "data/flooddata.db"



#this is no longer used but I left it in there and will delete later
def insertObservationIntoTable(observation_list):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_with_param = """
        INSERT OR IGNORE INTO observations
            (observation_time, observation_stage, observation_flow, observation_gauge_id)
            VALUES (?, ?, ?, ?);
                          """
        cursor.executemany(sqlite_insert_with_param, observation_list)
        sqliteConnection.commit()
        print("Query executed successfully")

        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")


def insertIntoTable(tuple_list, table):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite - " + flood_db)
        sqlite_insert_query = "None"
        forecast_insert_query = """
        INSERT INTO projections
            (projection_time_added,
            projection_time,
            projection_stage,
            projection_flow,
            projection_gauge_id)
            VALUES (?, ?, ?, ?, ?);
        """

        obs_insert_query = """
        INSERT INTO observations
            (observation_time,
            observation_stage,
            observation_flow,
            observation_gauge_id)
            VALUES (?, ?, ?, ?);
            """

        if table == "observations":
            sqlite_insert_query = obs_insert_query
        elif table == "forecasts":
            sqlite_insert_query = forecast_insert_query
        else:
            print("Error: didn't find table query in insertIntoTable")
            sys.exit()
        cursor.executemany(sqlite_insert_query, tuple_list)
        sqliteConnection.commit()
        print("Query executed successfully")

        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")


def get_gauges(flood_db):
    tempDB = sqlite3.connect(flood_db)
#    stored_obs_list = []
    gauge_list = tempDB.execute("""
        SELECT
            gauge_id
        FROM
            gauges
            """).fetchall()
    print("You've got "+ str(len(gauge_list))+ " gauges that we like to look up.")
    tempDB.close()
    return gauge_list

# pulls the data from the internet website
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


# Queries the DB to get the existing observation records
def get_stored_observations(flood_db):
    tempDB = sqlite3.connect(flood_db)
#    stored_obs_list = []
    stored_observations = tempDB.execute("""
        SELECT
            observation_time,
            observation_stage,
            observation_flow,
            observation_gauge_id
        FROM
            observations

            """).fetchall()
    print("You've got "+ str(len(stored_observations))+ " observations stored.")
    tempDB.close()
    return stored_observations

# Queries the DB to get the stored forecasts
def get_stored_forecasts(flood_db):
    tempDB = sqlite3.connect(flood_db)
    stored_forecasts = tempDB.execute("""
        SELECT
            projection_time_added,
            projection_time,
            projection_stage,
            projection_flow,
            projection_gauge_id
        FROM
            projections

            """).fetchall()
    print("You've got "+ str(len(stored_forecasts))+ " forecasts stored.")
    tempDB.close()
    return stored_forecasts


#Parses the online observations and returns as list of tuples
def parseobservations(tree):
    obs_list = []
    print("+++++Parsing Observations ++++++")
    for elem in tree.iterfind("observed/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
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



# Parses the online forecasts and returns as list of tuples
def parseforecast(tree):
    forecast_list = []
    forec_issue_time = tree.find("forecast").attrib.get("issued")
    forec_gauge_id_value = root.attrib.get("id")
    for elem in tree.iterfind("forecast/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
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



# Removes entries from parsed online list that already exist in DB
def remove_duplicates(list1, list2):
    for element in list1:
        if element in list2:
            list2.remove(element)
    return list2

# this is kinda like the main and ties most of the other crap
# together but inside the for loop from main

def pull_and_write_main(which_gauge):
    print("_____________________________")
    print("running pull and write for "+ which_gauge)
    tree = getonlinedata(which_gauge)

    stored_observations = get_stored_observations(flood_db)
    print("we've got "+str(len(stored_observations))+ " stored observations in the DB")
    forecast_list = parseforecast(tree)
    print("we've got "+ str(len(forecast_list))+ " forecasts entries that we found online")
    observation_list =   parseobservations(tree)
    stored_forecasts = get_stored_forecasts(flood_db)
    deduped_forec_list = remove_duplicates(stored_forecasts, forecast_list)
    print("we have "+ str(len(deduped_forec_list))+ " fresh forecasts to add")
    deduped_obs_list = remove_duplicates(stored_observations, observation_list)
    print("we have "+ str(len(deduped_obs_list))+ " fresh observations to add")
    if len(deduped_obs_list)>0:
        insertIntoTable(deduped_obs_list, "observations")
    if len(deduped_forec_list)>0:
        insertIntoTable(deduped_forec_list, "forecasts")
    print("_____________________________")



def main():
    gauge_list = get_gauges(flood_db)
    print(gauge_list)
    for gauge in gauge_list:
        pull_and_write_main(gauge[0])



main()
print( "This whooooole thing took ", time.time() - start_time, "to run")
