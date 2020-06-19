This file lists the module dependencies for the project and their licenses.

1. Most of this module code is **not** redistributed, either in source or binary
form. Instead, it is downloaded automatically using package managers and linked
from the code. The module download includes the license and appropriate credit.

1. So our primary check here is for modules which do not have a license, or
which are GPL licensed.

## Modified modules
Although the majority of the dependencies are unmodified, we have copied and
modified a few modules. The modified modules comply with the terms of the
license by including their license information.

| Module | License | Repository |
|--------|---------|------------|
| Bottle | MIT     | http://bottlepy.org |
| 

### Python unmodified modules installed from `environment.yml`

| Module | License | Repository |
|--------|---------|------------|
| arrow  | Apache  | https://github.com/crsmithdev/arrow/ |
| attrdict | MIT   | https://github.com/bcj/AttrDict/ |
| cheroot  | BSD 3-clause | https://github.com/cherrypy/cheroot |
| future   | MIT   | https://github.com/PythonCharmers/python-future |
| google-auth | Apache | https://github.com/googleapis/google-auth-library-python |
| numpy    | BSD 3-clause | https://github.com/numpy/numpy |
| pandas   | BSD 3-clause | https://github.com/pandas-dev/pandas |
| pip      | MIT | https://github.com/pypa/pip |
| python-dateutil | BSD 3-clause | https://github.com/dateutil/dateutil/ |
| pytz | MIT | http://pythonhosted.org/pytz/ |
| requests | Apache | https://github.com/psf/requests |
| scikit-learn | BSD 3-clause | http://scikit-learn.org/ |
| scipy | BSD 3-clause | http://www.scipy.org/ |
| utm | MIT | https://github.com/Turbo87/utm |
| jwcrypto | LGPL | https://github.com/latchset/jwcrypto |
| pyfcm | MIT | https://github.com/olucurious/pyfcm |
| pygeocoder | BSD | https://bitbucket.org/xster/pygeocoder |
| pymongo | Apache | http://github.com/mongodb/mongo-python-driver |

### Redistributed Javascript dependencies

These dependencies were checked in as part of the PR that created the webapp
https://github.com/e-mission/e-mission-server/pull/267
The commit
(https://github.com/e-mission/e-mission-server/pull/267/commits/6da89beb3d5e851ac9c78e7ec2bf8b80fc2154b1)
does not have any details of why I checked them in instead of using bower. Just
listing them for now. TODO: this should be cleaned up later 🚧

| `webapp/www/lib/angular-animate` | MIT |
| `webapp/www/lib/angular-sanitize` | MIT |
| `webapp/www/lib/angular-simple-logger` | MIT (from `bower.json`) |
| `webapp/www/lib/angular-ui-router` | MIT |
| `webapp/www/lib/angular` | MIT |
| `webapp/www/lib/ionic` | MIT (from [`bower.json`](https://github.com/ionic-team/ionic-bower/blob/v1.3.0/bower.json)) |
| `webapp/www/lib/leaflet` | BSD **2-clause** |
| `webapp/www/lib/simple-leaflet-plugins/leaflet-heat.js` | BSD **2-clause** |
| `webapp/www/lib/webapp/www/lib/ui-leaflet` | MIT |

### Unmodified Javascript dependencies installed using bower

| `webapp/www/lib/moment` | MIT |
| `webapp/www/lib/angular-bootstrap` | MIT |
| `webapp/www/lib/angular-nvd3` | MIT |
| `webapp/www/lib/angular-qrcode` | MIT |

pseudocode:
* DTW courtesy http://jeremykun.com/2012/07/25/dynamic-time-warping/ and https://gist.github.com/socrateslee/3265694
* LCS courtesy http://rosettacode.org/wiki/Longest_common_subsequence#Python

