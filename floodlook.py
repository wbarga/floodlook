import xml.etree.ElementTree as ET
import urllib.request
import xmltodict
import sys
import sqlite3


### this will create a connection to the Sqlite # DB
### except it doesn't do anything until I do the other stuff
def create_connection(dbfile):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn




url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage=mrow1&output=xml"
response = urllib.request.urlopen(url)
tree = ET.parse(response)
root = tree.getroot()
#print(list(root.text))

guage_id = root.attrib.get("id")
gen_time = root.attrib.get("generationtime")
print(gen_time)
sys.exit()
#prints each observed reading
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

for elem in tree.iterfind("forecast/datum"):
    stage = "None"
    datetime = "None"
    flow = "None"
    for child in elem:
        if child.attrib.get("name") == "Stage":
            stage = str(child.text)
        if child.attrib.get("timezone") == "UTC":
            datetime = str(child.text)
            datetime = utc_to_local(datetime)
        if child.attrib.get("name") == "Flow":
            flow = str(child.text)
    print("Forecast:")
    print("Date and time: "+ datetime)
    print("Flood Stage: " + stage)
    print("Flow: "+ flow)
    print("_____________________")
