e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the [e-mission-phone repo](https://github.com/amplab/e-mission-phone)

The backend in turn consists of two parts - a summary of their code structure is shown below.
The webapp supports a REST API, and accesses data from the database to fulfill
the queries.
![][CFC_WebApp_Structure]

A set of background scripts pull the data from moves, and preprocess results to
ensure reasonable performance.
![][CFC_DataCollector_Structure]

## Dependencies: ##
-------------------

### Database: ###
1. Install [Mongodb](http://www.mongodb.org/) (Note: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot)
Note: If you are using OSX: You want to install homebrew and then use homebrew to install mongodb. Follow these instruction on how to do so ---> (http://docs.mongodb.org/manual/tutorial/install-mongodb-on-os-x/)

1. Start it at the default port
    $ mongod
Ubuntu: http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

### Installing Pip for Mac: ###
1. Download get-pip.py (https://pip.pypa.io)
2. Install pip:
    $ python get-pip.py

### Python: ###

    (You may need super user permissions to run these commands)
    $ pip install -r requirements.txt 
    # If you are running this in production over SSL, copy over the cherrypy-wsgiserver
    $ cp api/wsgiserver2.py <dist-packages>/cherrypy/wsgiserver/wsgiserver2.py

### Using Pip on Windows: ###
    # To install pip:
    # Download or save get-pip.py file: https://bootstrap.pypa.io/get-pip.py
    # From download directory, run:
    $ python get-pip.py
    
    # To add pip to path (replace 'PythonXX' with actual Python directory name, e.g. 'Python27'):
    $ python C:\PythonXX\Tools\Scripts\win_add2path.py
    
    # Finally, to install modules as above:
    $ python -m pip install -r requirements.txt

## Development: ##
-------------------
In order to test out changes to the webapp, you should make the changes
locally, test them and then push. Then, deployment is as simple as pulling from
the repo to the real server.

Here are the steps for doing this:

1. Copy config.json.localhost.android or config.json.localhost.ios to
config.json, depending on which platform you are testing against.

1. Copy keys.json.sample to keys.json, register for the appropriate keys, and
fill them in

1. Start the server

        $ cd CFC_WebApp
        $ python api/cfc_webapp.py

   You may need to install some of the python dependencies from above
   Amongst the dependencies include:
   -cherrypy
   -pygeocoder

1. Browse to 
[http://localhost:8080](http://localhost:8080)
or connect to it using the phone app

1. Run unit tests
        $ cd CFC_WebApp
        $ python tests/TestCarbon.py
        $ python tests/TestTripManager.py
You might get an ImportError for the module utils. In this case you have to add PYTHONPATH to have the current directory in it. Try running "PYTHONPATH=. python tests/TestCarbon.py" instead.

1. If you need to run any of the backend scripts, copy the config.json and
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

* Google Developer Console
  - Android key
  - iOS key
  - webApp key
* Moves app
  - Client key
  - Client secret

And then copy over the sample files from these locations and replace the values appropriately:

* CFC\_WebApp/keys.json
* CFC\_DataCollector/keys.json


TROUBLESHOOTING:

1. If a python execution fails to import a module, make sure to add current directory to your PYTHONPATH. 

1. If starting the server gives a CONNECTION_ERROR, make sure MongoDB is actively running when you attempt to start the server. Make sure to md c:\data\db\ if dbpath does not exist.

## Design decisions: ##
----------
This site is currently designed to support commute mode tracking and
aggregation. There is a fair amount of backend work that is more complex than
just reading and writing data from a database. So we are not using any of the
specialized web frameworks such as django or rails.

Instead, we have focused on developing the backend code and exposing it via a
simple API. I have maintained separation between the backend code and the API
glue so that we can swap out the API glue later if needed.

The API glue is currently [Bottle](http://bottlepy.org/docs/dev/index.html), which is a single file webapp framework. I
chose [Bottle](http://bottlepy.org/docs/dev/index.html) because it was simple, didn't use a lot of space, and because it
wasn't heavy weight, could easily be replaced with something more heavyweight
later.

The front-end is javascript based - I will experiment with Angular later, but
right now, it uses jquery and NVD3, which is a wrapper layer that makes it
easier to display simple graphs using D3.


## Deployment: ##
----------
If you are running this in production, you should really run it over SSL.  We
use cherrypy to provide SSL support. The default version of cherrypy in the
anaconda distribution had some issues, so I've checked in a working version
of the wsgiserver file.

TODO: clean up later

    $ cp api/wsgiserver2.py <dist-packages>/cherrypy/wsgiserver/wsgiserver2.py

[CFC_WebApp_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/CFC_WebApp_Structure.png
[CFC_DataCollector_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/CFC_DataCollector_Structure.png
