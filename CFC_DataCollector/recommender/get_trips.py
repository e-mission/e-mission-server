import os, sys
print ("HERE")
print (os.getcwd())
print ("THERE")
sys.path.append("%s/../CFC_WebApp/" % os.getcwd())
from main.userclient import getClientSpecificQueryFilter
from main.get_database import get_section_db

class TripIterator:
    """
    Extracts Trip objects that can be passed
    into the Google Maps API section
    """
    
    def __init__(self, user_uuid, queryList = []):                
        # Query section_db with queryList and 
        # instantiate the cursor object 
        # returned as an instance variable
        clientSpecificQuery = getClientSpecificQueryFilter(user_uuid)
        completeQuery = clientSpecificQuery + queryList
        Sections = get_section_db()
        self.cur = Sections.find({"and": completeQuery})

    def __iter__(self):
        return self
        
    def next(self):
        # Get next record from cursor and 
        # cast it to a Trip object, or one 
        # of the Trip subclasses.        
        trip = next(self.cur)
        if trip is None:
            return None
        return Trip(trip)
