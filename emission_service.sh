#!/bin/sh
. /root/anaconda3/etc/profile.d/conda.sh
conda activate emission
cd '/var/emission/e-mission-server/'
PYTHONPATH=. python -u emission/net/api/cfc_webapp.py > /var/tmp/webserver_console.log &
