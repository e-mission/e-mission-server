e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the [e-mission-phone
repo](https://github.com/amplab/e-mission-phone)

The backend in turn consists of two parts - a summary of their code structure is shown below.
-![][Python_Structure]
The webapp supports a REST API, and accesses data from the database to fulfill
the queries.  A set of background scripts pull the data from external sources, and
preprocessing results ensures reasonable performance.

## Dependencies: ##
-------------------

### Database: ###
1. Install [Mongodb](http://www.mongodb.org/) (Note: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot)
Note: If you are using OSX: You want to install homebrew and then use homebrew to install mongodb. Follow these instruction on how to do so ---> (http://docs.mongodb.org/manual/tutorial/install-mongodb-on-os-x/)

2. Start it at the default port
    $ mongod
Ubuntu: http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

### Python distribution ###
We will use a distribution of python that is optimized for scientific
computing. The [anaconda](https://store.continuum.io/cshop/anaconda/)
distribution is available for a wide variety of platforms and includes the
python scientific computing libraries (numpy/scipy/scikit-learn) along with
native implementations for performance. Using the distribution avoids native
library inconsistencies between versions.

The distribution also includes its own version of pip, and a separate package
management tool called 'conda'.

After you install the anaconda distribution, please ensure that it is in your
path, and you are using the anaconda versions of common python tools such as
`python` and `pip`, e.g.

    $ which python
    /Users/shankari/OSS/anaconda/bin/python

    $ which pip
    /Users/shankari/OSS/anaconda/bin/pip

### Python dependencies: ###

    (You may need super user permissions to run these commands)
    $ pip install -r requirements.txt 
    # If you are running this in production over SSL, copy over the cherrypy-wsgiserver
    $ cp api/wsgiserver2.py <dist-packages>/cherrypy/wsgiserver/wsgiserver2.py

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

1. Start the server (Note: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot)

        $ cd CFC_WebApp
        $ PYTHONPATH=../base:../CFC_DataCollector/:. python api/cfc_webapp.py

   You may need to install some of the python dependencies from above
   Amongst the dependencies include:
   -cherrypy
   -pygeocoder

1. Browse to 
[http://localhost:8080](http://localhost:8080)
or connect to it using the phone app

1. You might also want to create some users, e.g.
        $ cd ..../e-mission-server
        $ ./e-mission-py.bash bin/create_test_user.py shankari@berkeley.edu

1. If you are testing the data collection of sensor other than location, you
want to enable them.
        $ cd ..../e-mission-server
        $ cp bin/load_config.json.sample bin/load_config.json
        $ vi bin/load_config.json # edit it
        $ ./e-mission-py.bash bin/load_config.py bin/load_config.json

### Running unit tests ###

1. Make sure that the anaconda python is in your path

        $ which python
        /Users/shankari/OSS/anaconda/bin/python

1. Run all tests.

        $ ./runAllTests.sh

1. If you get import errors, you may need to add the current
directory to PYTHONPATH.

        $ PYTHONPATH=. ./runAllTests.sh

## Analysis ##
Several exploratory analysis scripts are checked in as ipython notebooks into
`emission/analysis/notebooks`. All data in the notebooks is from members of the
research team who have provided permission to use it. The results in the
notebooks cannot be replicated in the absence of the raw data, but they can be
run on data collected from your own instance as well.

The notebooks are occasionally modified and simplified as code is moved out of
them into utility functions. Original versions of the notebooks can be obtained
by looking at other notebooks with the same name, or by looking at the history
of the notebooks.

### Running backend analysis scripts ###

1. If you need to run any of the backend scripts, copy the config.json and
keys.json files to that directory as well, and run the scripts:

        cd CFC_DataCollector
        cp ../CFC_WebApp/config.json .
        cp ../CFC_WebApp/keys.json .

        python moves/collect.py
        python modeinfer/pipeline.py
   
These repositories don't have the associated client keys checked in for
security reasons.

### Getting keys ###

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

## TROUBLESHOOTING: ##

1. If a python execution fails to import a module, make sure to add current
directory to your PYTHONPATH.

1. If starting the server gives a CONNECTION\_ERROR, make sure MongoDB is
actively running when you attempt to start the server.

1. On windows, if you get an error that `dbpath does not exist`, make sure to 
    % md c:\data\db\ 

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

Also, now that we decode the JWTs locally, we need to use the oauth2client,
which requires the PyOpenSSL library. This can be installed on ubuntu using the
python-openssl package, but then it is not accessible using the anaconda
distribution. In order to enable it for the conda distribution as well, use

    $ conda install pyopenssl

Also, installing via `requirements.txt` does not appear to install all of the
requirements for the google-api-client. If you get mysterious "Invalid token"
errors for tokens that are correctly validated by the backup URL method, try to
uninstall and reinstall with the --update option.

    $ pip uninstall google-api-python-client
    $ pip install --upgrade google-api-python-client

If you are running the server on shared, cloud infrastructure such as AWS, then
note that the data is accessible by AWS admins by directly looking at the disk.
In order to avoid this, you want to encrypt the disk. You can do this by:
- using an encrypted EBS store, but this doesn't appear to allow you to specify
  your own encryption key
- using a normal drive that is encrypted using cryptfs (http://sleepyhead.de/howto/?href=cryptpart, https://wiki.archlinux.org/index.php/Dm-crypt/Encrypting_a_non-root_file_system)

In either of these cases, you need to reconfigure mongod.conf to point to data
and log directories in the encrypted volume.

[Python_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/e-mission-server-module-structure.png
