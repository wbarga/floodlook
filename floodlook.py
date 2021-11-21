import xml.etree.ElementTree as ET
import urllib.request
import xmltodict
import sys
import sqlite3


### this will create a connection to the Sqlite # DB
### except it doesn't do anything until I do the other stuff
flood_db = "data/flooddata.db"

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
### this can test the DB connection by printing the full observations table
#rowcount = 2
#readSqliteTable(rowcount)
#Just for testing
#sqlitetestconnect()



def insertObservationIntoTable(observation_time, observation_stage, observation_flow):
    try:
        sqliteConnection = sqlite3.connect(flood_db)
        cursor = sqliteConnection.cursor()
        print("Connected to SQLite")

        sqlite_insert_with_param = """INSERT INTO observations
                          (observation_time, observation_stage, observation_flow)
                          VALUES (?, ?, ?);"""

        data_tuple = (observation_time, observation_stage, observation_flow)
        cursor.execute(sqlite_insert_with_param, data_tuple)
        sqliteConnection.commit()
        print("Python Variables inserted successfully into SqliteDb_developers table")

        cursor.close()

    except sqlite3.Error as error:
        print("Failed to insert Python variable into sqlite table", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

#insertObservationIntoTable("another time", 345, 356)
#insertObservationIntoTable("yet anoehrtera", 456, 234)


#sys.exit("Stopped bc we're testing")



url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage=mrow1&output=xml"
tree = "null"
root = "null"

def getdata(url):
    response = urllib.request.urlopen(url)
    tree = ET.parse(response)
    root = tree.getroot()
    #print(list(root.text))

    guage_id = root.attrib.get("id")
    gen_time = root.attrib.get("generationtime")
    print(gen_time)

#sys.exit()
#prints each observed reading
def writeobservations():
    for elem in tree.iterfind("observed/datum"):
        stage = "None"
        datetime = "None"
        flow = "None"
        for child in elem:
            if str(child.attrib.get("name")) == "Stage":
                stage = str(child.text)
            if child.attrib.get("timezone") == "UTC":
                datetime = str(child.text)
            if str(child.attrib.get("name")) == "Flow":
                flow = str(child.text)
        print("Observation:")
        print("Date and time: "+ datetime)
        print("Flood Stage: " + stage)
        print("Flow: "+ flow)
        print("_____________________")
        insertObservationIntoTable(datetime, stage, flow)

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


getdata(url)
writeobservations()
