## A wrapper for the 511 traffic api
import xml.etree.ElementTree as ET
import urllib2, datetime

def get_orig(orig_city):
	url = "http://services.my511.org/traffic/getoriginlist.aspx?token=76495c63-cb01-4aac-af28-f717ffee11c8"
	response = urllib2.urlopen(url)
	tree = ET.parse(response)
	root = tree.getroot()
	for child in root:
		if child[0].text.lower() in orig_city.lower():
			 return child[3].text
	#raise Exception("No traffic data found")
        return "0"

def get_destination(orig_city, dest_city):
	url = 'http://services.my511.org/traffic/getdestinationlist.aspx?token=76495c63-cb01-4aac-af28-f717ffee11c8&o=%s' % get_orig(orig_city)
	response = urllib2.urlopen(url)
	tree = ET.parse(response)
	root = tree.getroot()
	for child in root:
		if child[0].text.lower() in dest_city.lower():
			return child[3].text
	#raise Exception("No traffic data found")
        return "0"

def get_travel_time(orig_city, dest_city):
	url = 'http://services.my511.org/traffic/getpathlist.aspx?token=76495c63-cb01-4aac-af28-f717ffee11c8&o=%s&d=%s' % (get_orig(orig_city), get_destination(orig_city, dest_city))
	print url
	response = urllib2.urlopen(url)
	tree = ET.parse(response)
	root = tree.getroot()
        if orig_city == "0" or dest_city == "0":
            return datime.timedelta(seconds=0)
        try:
            return datetime.timedelta(minutes=int(root[0][0].text))
        except Exception:
            return datetime.timedelta(seconds=0)


