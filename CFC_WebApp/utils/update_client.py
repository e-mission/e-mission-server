from dao.client import Client
import sys

def update_entry(clientName):
  newKey = Client(clientName).update(createKey = False)
  print "%s: %s" % (clientName, newKey)

if __name__ == '__main__':
  # Deal with getopt here to support -a
  update_entry(sys.argv[1])
