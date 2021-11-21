import xml.etree.ElementTree as ET
import urllib.request
import xmltodict

url = "https://water.weather.gov/ahps2/hydrograph_to_xml.php?gage=mrow1&output=xml"
response = urllib.request.urlopen(url)
tree = ET.parse(response)
root = tree.getroot()
#print(list(root.text))



#prints each observed reading
for elem in tree.iterfind("observed/datum"):
    for child in elem:
#        print(type(child))
        print(child.tag, child.attrib.get("name"), child.text)

    print("_____________________")
