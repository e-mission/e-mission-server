e-mission is a project to gather data about user travel patterns using phone
apps, and use them to provide an personalized carbon footprint, and aggregate
them to make data available to urban planners and transportation engineers.

If you are here to use ***Zephyr***, which uses the e-mission platform to
evaluate power/accuracy tradeoffs for background sensed data, please see the
[zephyr-specific README and examples](https://github.com/e-mission/e-mission-server/tree/master/zephyr).

It has two components, the backend server and the phone apps. This is the
backend server - the phone apps are available in the [e-mission-phone
repo](https://github.com/amplab/e-mission-phone)

[![Build Status](https://travis-ci.org/shankari/e-mission-server.svg?branch=master)](https://travis-ci.org/shankari/e-mission-server) ![test-with-docker](https://github.com/e-mission/e-mission-server/workflows/test-with-docker/badge.svg) ![ubuntu-only-test-with-manual-install](https://github.com/e-mission/e-mission-server/workflows/ubuntu-only-test-with-manual-install/badge.svg) ![osx-ubuntu-manual-install](https://github.com/e-mission/e-mission-server/workflows/osx-ubuntu-manual-install/badge.svg)

**Issues:** Since this repository is part of a larger project, all issues are tracked [in the central docs repository](https://github.com/e-mission/e-mission-docs/issues). If you have a question, [as suggested by the open source guide](https://opensource.guide/how-to-contribute/#communicating-effectively), please file an issue instead of sending an email. Since issues are public, other contributors can try to answer the question and benefit from the answer.

The backend in turn consists of two parts - a summary of their code structure is shown below.
-![][Python_Structure]
The webapp supports a REST API, and accesses data from the database to fulfill
the queries.  A set of background scripts pull the data from external sources, and
preprocessing results ensures reasonable performance.

## Installation: ##
----------
- For **deployers** (i.e. if you want to primarily *use* the system as opposed to modify/develop it, the [docker installation](https://github.com/e-mission/e-mission-docker) is probably the easiest way to get started.
- For **builders** (i.e. if you want to write new scripts or modify existing scripts) the [manual install](https://github.com/e-mission/e-mission-docs/blob/master/docs/install/manual_install.md) will make it easier to edit files directly on your local filesystem. Make sure to use a POSIX-compliant CLI; you may want to look into [gitbash](https://openhatch.org/missions/windows-setup/install-git-bash) or similar on Windows.

## Additional Documentation: ##
----------
Additional documentation has been moved to its own repository [e-mission-docs](https://github.com/e-mission/e-mission-docs). 

The API glue is currently [Bottle](http://bottlepy.org/docs/dev/index.html), which is a single file webapp framework. I
chose [Bottle](http://bottlepy.org/docs/dev/index.html) because it was simple, didn't use a lot of space, and because it
wasn't heavy weight, could easily be replaced with something more heavyweight
later.

The front-end is javascript based. In order to be consistent with the phone, it
also uses angular + ionic. javascript components are largely managed using
bower.

## Deployment: ##
----------
This is fairly complex and is under active change as we have more projects deploy their own servers with various configurations.
So I have moved it to the e-mission-server section in the e-mission-docs repo:
https://github.com/e-mission/e-mission-docs/blob/master/docs/install/deploying_your_own_server_to_production.md

[Python_Structure]: https://raw.github.com/amplab/e-mission-server/master/figs/e-mission-server-module-structure.png
