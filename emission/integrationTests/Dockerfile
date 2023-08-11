# python 3
FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y curl

# CHANGEME: Create the files that correspond to your configuration in the conf directory
# COPY conf/net/auth/google_auth.json /usr/src/app/conf/net/auth/google_auth.json
COPY start_integration_tests.sh /start_integration_tests.sh

ENV NOMINATIM_QUERY_URL='http://localhost:8080'

CMD ["/bin/bash", "/start_integration_tests.sh"]