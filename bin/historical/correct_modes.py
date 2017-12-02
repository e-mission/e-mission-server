from __future__ import print_function
# This script allows us to correct misclassified user confirmed trips.
# We suspect that the user misclassified them because they were inaccurately
# classified high confidence trips.
# We don't want to edit sections in a live database willy-nilly, so we create
# this script.  All corrections should happen through the script, and the
# associated correction file should be checked in for further reference

from get_database import get_section_db

# Handle comments as per:
# http://bugs.python.org/issue1225769
class CommentedFile:
    def __init__(self, f, commentstring="#"):
        self.f = f
        self.commentstring = commentstring

    def next(self):
        line = next(self.f)
        while line.startswith(self.commentstring):
            line = next(self.f)
        return line

    def __iter__(self):
        return self

def correctTrip(tripId, sectionId, correctedMode):
    get_section_db().update({'trip_id': tripId, 'section_id': sectionId},
        {'$set': {'corrected_mode': correctedMode}}, upsert = False)

def confirmCorrection(tripId, sectionId, userId, correctedMode):
    entry = get_section_db().find_one({'$and': [{'trip_id': tripId, 'section_id': sectionId}]})
    print ("Checking against entry %s" % entry)
    assert(entry['user_id'] == userId)
    assert(entry['corrected_mode'] == correctedMode)
    assert(entry['confirmed_mode'] != correctedMode)
    
def correctTrips(correctionFileName):
    import csv
    from uuid import UUID

    with open(correctionFileName, 'rb') as correctionFile:
        correctionReader = csv.reader(CommentedFile(correctionFile))
        for correction in correctionReader:
            print(correction)
            # We need to format the fields correctly
            tripId = correction[0]
            sectionId = int(correction[1])
            userId = UUID(correction[2])
            correctMode = int(correction[3])
            print("Correcting trip (trip_id = %s, section = %s) for user %s -> %s" % 
                (tripId, sectionId, userId, correctMode))
            correctTrip(tripId, sectionId, correctMode)
            confirmCorrection(tripId, sectionId, userId, correctMode)

if __name__ == '__main__':
    import sys

    if len(sys.argv) != 2:
        print("usage: correct_modes <correction file>")
        exit(1)

    correctTrips(sys.argv[1])
