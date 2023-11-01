# python 3
FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y curl

# CHANGEME: Create the files that correspond to your configuration in the conf directory

RUN echo "About to copy e-mission server code"
COPY start_integration_tests.sh/ /start_integration_tests.sh

CMD ["/bin/bash", "/start_integration_tests.sh"]