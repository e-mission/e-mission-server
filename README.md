e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the e-mission-phone repo.

This site is currently designed to support commute mode tracking and
aggregation. There is a fair amount of backend work that is more complex than
just reading and writing data from a database. So we are not using any of the
specialized web frameworks such as django or rails.

Instead, we have focused on developing the backend code and exposing it via a
simple API. I have maintained separation between the backend code and the API
glue so that we can swap out the API glue later if needed.

The API glue is currently bottle, which is a single file webapp framework. I
chose bottle because it was simple, didn't use a lot of space, and because it
wasn't heavy weight, could easily be replaced with something more heavyweight
later.

The front-end is javascript based - I will experiment with Angular later, but
right now, it uses jquery and NVD3, which is a wrapper layer that makes it
easier to display simple graphs using D3.

main: contains the main backend code
api: contains the bottle glue code
front: contains the javascript frontend

Python dependencies:
  $ pip install pymongo
  $ pip install requests
  $ pip install pytz
  $ pip install cherrypy
  # If you are running this in production over SSL, copy over the cherrypy-wsgiserver
  $ cp api/wsgiserver2.py <dist-packages>/cherrypy/wsgiserver/wsgiserver2.py
  $ pip install python-dateutil
  $ pip install pygeocoder
  $ pip install utm

Development:
In order to test out changes to the webapp, you should really make the changes
locally, test them and then push. Then, deployment is as simple as pulling on
the real server.

Here are the steps for doing this:
1) Copy config.json.localhost.android or config.json.localhost.ios to
config.json, depending on which platform you are testing against.

2) Copy keys.json.sample to keys.json, register for the appropriate keys, and
fill them in

3) Start the server
  $ cd CFC_WebApp
  $ python api/cfc_webapp.py

You may need to install some of the python dependencies from above

4) Browse to 
http://localhost:8080
or connect to it using the phone app

5) If you need to run any of the backend scripts, copy the config.json and
keys.json files to that directory as well, and run the scripts:
   cd CFC_DataCollector
   cp ../CFC_WebApp/config.json .
   cp ../CFC_WebApp/keys.json .

   python moves/collect.py
   python modeinfer/pipeline.py
   
These repositories don't have the associated client keys checked in for
security reasons.

If you are associated with the e-mission project and will be integrating with
our server, then you can get the key files from:
https://repo.eecs.berkeley.edu/git/users/shankari/e-mission-keys.git

If not, please get your own copies of the following keys:
  # Google Developer Console
    # Android key
    # iOS key
    # webApp key
  # Moves app
    # Client key
    # Client secret

And then copy over the sample files from these locations and replace the values appropriately:
  # CFC\_WebApp/keys.json
  # CFC\_DataCollector/keys.json
