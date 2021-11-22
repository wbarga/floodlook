import xml.etree.ElementTree as ET
import urllib.request
import xmltodict
import sys
import sqlite3



#Setting the global variables
flood_db = "data/flooddata.db"
# this sets the gauge to query, but need to remember to remove this later to make it check various
which_gauge = "mrow"
tree = "null"
root = "null"

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


def insertObservationIntoTable(obs_time_value, obs_stage_value, obs_flow_value, obs_gauge_id_value):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_with_param = """
        INSERT OR IGNORE INTO observations
            (observation_time, observation_stage, observation_flow, observation_gauge_id)
            VALUES (?, ?, ?, ?);
                          """
        data_tuple = (obs_time_value, obs_stage_value, obs_flow_value, obs_gauge_id_value)
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
    url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage="+ which_gauge +"1&output=xml"
    response = urllib.request.urlopen(url)
    tree = ET.parse(response)
    root = tree.getroot()
    #print(list(root.text))

    guage_id = root.attrib.get("id")
    gen_time = root.attrib.get("generationtime")
    print(gen_time)
    print(tree)


#prints each observed reading
def parseobservations():
    for elem in tree.iterfind("observed/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
        obs_gauge_id_value = root.attrib.get("id")
        for child in elem:
            if str(child.attrib.get("name")) == "Stage":
                stage = str(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if str(child.attrib.get("name")) == "Flow":
                flow = str(child.text)

#        print("Observation:")
#        print("Date and time: "+ datetime)
#        print("Flood Stage: " + stage)
#        print("Flow: "+ flow)
        print("_____________________")
        insertObservationIntoTable(datetime, stage, flow, obs_gauge_id_value)

def writeforecast():
    for elem in tree.iterfind("forecast/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
        for child in elem:
            if child.attrib.get("name") == "Stage":
                stage = str(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if child.attrib.get("name") == "Flow":
                flow = str(child.text)
        print("Forecast:")
        print("Date and time: "+ datetime)
        print("Flood Stage: " + stage)
        print("Flow: "+ flow)
        print("_____________________")


getdata(which_gauge)
print("Global Tree:" + str(tree))
parseobservations()
