from dao.client import Client
import sys

def update_entry(clientName, createKeyOpt):
  print "createKeyOpt = %s" % createKeyOpt
  newKey = Client(clientName).update(createKey = createKeyOpt)
  print "%s: %s" % (clientName, newKey)

if __name__ == '__main__':
  # Deal with getopt here to support -a
  update_entry(sys.argv[1], sys.argv[2] == "true" or sys.argv[2] == "True")
