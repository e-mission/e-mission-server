e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the [e-mission-phone
repo](https://github.com/amplab/e-mission-phone)

The current build status is:
[![Build Status](https://amplab.cs.berkeley.edu/jenkins/buildStatus/icon?job=e-mission-server)](https://amplab.cs.berkeley.edu/jenkins/view/E-Mission/job/e-mission-server/)

The backend in turn consists of two parts - a summary of their code structure is shown below.
-![][Python_Structure]
The webapp supports a REST API, and accesses data from the database to fulfill
the queries.  A set of background scripts pull the data from external sources, and
preprocessing results ensures reasonable performance.

## Dependencies: ##
-------------------

### Database: ###
1. Install [Mongodb](http://www.mongodb.org/)
  2. *Windows*: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot
  3. *OSX*: You want to install homebrew and then use homebrew to install mongodb. Follow these instruction on how to do so ---> (http://docs.mongodb.org/manual/tutorial/install-mongodb-on-os-x/)
  4. *Ubuntu*: http://docs.mongodb.org/manual/tutorial/install-mongodb-on-ubuntu/

2. Start it at the default port

     `$ mongod`

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
In order to test out changes to the webapp, you should make the changes locally, test them and then push. Then, deployment is as simple as pulling from the repo to the real server and changing the config files slightly.

Here are the steps for doing this:

1. On OSX, start the database  (Note: mongodb appears to be installed as a service on Windows devices and it starts automatically on reboot). 

        $ mongod

1. Start the server

        $ ./e-mission-py.bash emission/net/api/cfc_webapp.py

1. Test your connection to the server
  * Using a web browser, go to [http://localhost:8080](http://localhost:8080)
  * Using the iOS emulator, connect to [http://localhost:8080](http://localhost:8080)
  * Using the android emulator:
    * change `server.host` in `conf/net/api/webserver.conf` to 0.0.0.0, and 
    * connect the app to the special IP for the current host in the android emulator - [10.0.2.2](https://developer.android.com/tools/devices/emulator.html#networkaddresses)

### Loading test data ###

You may also want to load some test data.

1. Sample timeline data from the data collection eval is available in the [data-collection-eval repo](https://github.com/shankari/data-collection-eval).
2. You can choose to load either android data `results_dec_2015/ucb.sdb.android.{1,2,3}/timeseries/*` or iOS data `results_dec_2015/ucb.sdb.ios.{1,2,3}/timeseries/*`
3. Data is loaded using the `bin/debug/load_timeline_for_day_and_user.py`.
  * Running it with just a timeline file loads the data with the original user - e.g.

            $ cd ..../e-mission-server
            $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py /tmp/data-collection-eval/results_dec_2015/ucb.sdb.android.1/timeseries/active_day_2.2015-11-27
        
  * Running it with a timeline file and a user relabels the data as being from the specified user and then loads it - e.g.

            $ cd ..../e-mission-server
            $ ./e-mission-py.bash bin/debug/load_timeline_for_day_and_user.py /tmp/data-collection-eval/results_dec_2015/ucb.sdb.android.1/timeseries/active_day_2.2015-11-27 -u shankari@eecs.berkeley.edu
        
4. Note that loading the data retains the object IDs. This means that if you load the same data twice with different user IDs, then only the second one will stick. In other words, if you load the file as `user1@foo.edu` and then load the same file as `user2@foo.edu`, you will only have data for `user2@foo.edu` in the database.


### Creating fake user data ###

You may need a larger or more diverse set of data than the given test data supplies.
To create it you can run the trip generation script included in the project. 

The script works by creating random noise around starting and ending points of trips.

You can fill out options for the new data in emission/simulation/input.json. 
The different options are as follows
* radius - the number of kilometers of randomization around starting and ending points (the amount of noise)
* starting centroids - addresses you want trips to start around, as well as a weight defining the relative probability a trip will start there
* ending centroids - addresses you want trips to end around, as well as a weight defining the relative probability a trip will end there
* modes - the relative probability a user will take a trip with the given mode
* number of trips - the amount of trips the simulation should create

run the script with 
    
    $ python emission/simulation/trip_gen.py <user_name>

Because this user data is specifically designed to test our tour model creation, you can create fake tour models easily by running the make_tour_model_from_fake_data function in emission/storage/decorations/tour_model_queries.py


### Running the analysis pipeline ###

Once you have loaded the timeline, you probably want to segment it into trips and sections, smooth the sections, generate a timeline, etc. We have a unified script to do all of those, called the intake pipeline. You can run it like this.

    $ ./e-mission-py.bash bin/intake_stage.py
    
Once the script is done running, places, trips, sections and stops would have been generated and stored in their respective mongodb tables, and the timelines for the last 7 days have been stored in the usercache.

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

## JS Testing ##

From the webapp directory

    $ npm install karma --save-dev
    $ npm install angular-mocks
    $ npm install karma-jasmine karma-chrome-launcher --save-dev

Write tests in www/js/test
If you're interested in having karma in your path and globally set, run 

    $ npm install -g karma-cli

To run tests if you have karma globally set, run 

    $ karma start my.conf.js 
    
in the webapp directory. If you didn't run the -g command, you can run
tests with 

    $ /node_modules/karma/bin/karma start
    
in the webapp directory


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
If you want to use this for anything other than deployment, you should really run it over SSL. In order to make the development flow smoother, *if the server is running over HTTP as opposed to HTTPS, it has no security*. The JWT basically consists of the user email *in plain text*. This means that anybody who knows a users' email access can download their detailed timeline. This is very bad.

<font color="red">If you are using this to store real, non-test data, use SSL right now</font> 

If you are running this in production, you should really run it over SSL. We
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

### Getting keys ###

If you are associated with the e-mission project and will be integrating with
our server, then you can get the key files from:
https://repo.eecs.berkeley.edu/git/users/shankari/e-mission-keys.git

If not, please get your own copies of the following keys:

* Google Developer Console (stored in conf/net/keys.json)
  - iOS key (`ios_client_key`)
  - webApp key (`client_key`)
* Parse  (coming soon)

[Python_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/e-mission-server-module-structure.png
