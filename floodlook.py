import xml.etree.ElementTree as ET
import urllib.request
import xmltodict
import sys
import sqlite3



#Setting the global variables
flood_db = "data/flooddata.db"
# this sets the gauge to query, but need to remember to remove this later to make it check various
which_gauge = "MROW1"
#tree = "null"
#root = "null"

### this will create a connection to the Sqlite # DB
### except it doesn't do anything until I do the other stuff
def sqlitetestconnect():
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Database created and Successfully Connected to SQLite")

        sqlite_select_Query = "select sqlite_version();"
        cursor.execute(sqlite_select_Query)
        record = cursor.fetchall()
        print("SQLite Database Version is: ", record)
        cursor.close()

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

#Should print a table entry - just for testing

def readSqliteTable(rowcount):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_select_query = """SELECT * from observations"""
        cursor.execute(sqlite_select_query)
        records = cursor.fetchmany(rowcount)
        print("Total rows are:  ", len(records))
        print("Printing " + str(rowcount) + " rows")
        print("\n")
        for row in records:
            print("observation_time: ", row[0])
            print("observation_stage", row[1])
            print("observation_flow", row[2])
            print("PK ", row[3])
            print("\n")

        cursor.close()

    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")
### uncomment these to do the basic sql connection tests
#rowcount = 2
#readSqliteTable(rowcount)
#Just for testing
#sqlitetestconnect()
#sys.exit("Stopped bc we're testing")


def insertObservationIntoTable(observation_tuple):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_with_param = """
        INSERT OR IGNORE INTO observations
            (observation_time, observation_stage, observation_flow, observation_gauge_id)
            VALUES (?, ?, ?, ?);
                          """
        data_tuple = observation_tuple
        cursor.execute(sqlite_insert_with_param, data_tuple)
        sqliteConnection.commit()
        print("Query executed successfully")

        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")


def getdata(which_gauge):
    global tree, root
    print("Executing getdata")
    url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage="+ which_gauge +"&output=xml"
    response = urllib.request.urlopen(url)
    tree = ET.parse(response)
    root = tree.getroot()
    #print(list(root.text))

    guage_id = root.attrib.get("id")
    gen_time = root.attrib.get("generationtime")
    #print(gen_time)
    #print(tree)
    return tree

# Queries the DB to get the current observation_stage

def get_stored_observations(flood_db):
    tempDB = sqlite3.connect(flood_db)
    stored_obs_list = []
#    tempDB.row_factory = sqlite3.Row
    stored_observations = tempDB.execute("""
        SELECT
            observation_time,
            observation_stage,
            observation_flow,
            observation_gauge_id
        FROM
            observations

            """).fetchall()
#    for item in stored_observations:
#        stored_observations.append({k: item[k] for k in item.keys()})
    print("You've got "+ str(len(stored_observations))+ " observations stored.")
    tempDB.close()
    return stored_observations


#prints each observed reading
def parseobservations(tree):
    obs_list = []
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
#            obs_entry_list = {"stage": stage, "datetime": datetime, "flow": flow, "gauge_id": obs_gauge_id_value}
            obs_entry_list = (datetime, stage, flow, obs_gauge_id_value)
#        print(obs_entry_list)
#        print("Observation:")
#        print("Date and time: "+ datetime)
#        print("Flood Stage: " + stage)
#        print("Flow: "+ flow)
#        print("_____________________")
        obs_list.append(obs_entry_list)
#        insertObservationIntoTable(datetime, stage, flow, obs_gauge_id_value)
    print(len(obs_list))
#    for item in obs_list:
#        print(item)
    return obs_list
def parseforecast(tree):
    forecast_list = []
    for elem in tree.iterfind("forecast/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
        forec_gauge_id_value = root.attrib.get("id")
        for child in elem:
            if child.attrib.get("name") == "Stage":
                stage = str(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if child.attrib.get("name") == "Flow":
                flow = str(child.text)
# this is for when I'm making it a dictionary but still figuring that out
#            forecast_entry_list = {"datetime":datetime, "stage": stage, "flow": flow, "guage_id": forec_gauge_id_value}
#            forecast_entry_list = (datetime, stage, flow, forec_guage_id_value)
#        print("Forecast:")
#        print("Date and time: "+ datetime)
#        print("Flood Stage: " + stage)
#        print("Flow: "+ flow)
#        print("_____________________")
        forecast_list.append(forecast_entry_list)
#    print(len(forecast_list))
#    print(forecast_list)
    return forecast_list

def remove_duplicates(list1, list2):
    for element in list1:
        if element in list2:
            list2.remove(element)
    return list2

def main():
    print("executing main")
    tree = getdata(which_gauge)
    stored_observations = get_stored_observations(flood_db)
    print("______________STORED OBSERVATIONS____________")
    for item in stored_observations:
        print(item)
    print("____________ONLINE OBSERVATIONS_____________")
    observation_list =   parseobservations(tree)
#    forecast_list = parseforecast(tree)
#    print(len(forecast_list))
    for item in observation_list:
#        print("observation")
        print(item)
#    for item in forecast_list:
#        print(item)
    deduped_obs_list = remove_duplicates(stored_observations, observation_list)
    print("deduped:")
    for item in deduped_obs_list:
        insertObservationIntoTable(item)
        print(item)
main()
