# python 3
FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y curl wget

# CHANGEME: Create the files that correspond to your configuration in the conf directory
# COPY conf/net/auth/google_auth.json /usr/src/app/conf/net/auth/google_auth.json

ENV DOCKERIZE_VERSION v0.5.0
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

RUN echo "About to copy e-mission server code"
COPY start_integration_tests.sh/ /start_integration_tests.sh

# CMD ["python", "../individual_tests/TestNominatim.py"]
CMD ["/bin/bash", "/start_integration_tests.sh"]