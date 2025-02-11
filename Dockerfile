# python 3
FROM ubuntu:jammy-20240227

MAINTAINER K. Shankari (shankari@eecs.berkeley.edu)

ADD https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/certs/

WORKDIR /usr/src/app

RUN apt-get -y -qq update
RUN apt-get install -y -qq curl
RUN apt-get install -y -qq wget
RUN apt-get install -y -qq git

# install nano and vim for editing
# RUN apt-get -y install nano vim

# install jq to parse json within bash scripts
RUN apt-get install -y jq

# Upgrade to resolve KEVs
# TODO: Switch to more recent LTS
# we are currently on jammy (22.04)
# most recent LTS is numbat (24.04)
RUN apt-get -y upgrade

# cleanup
RUN apt-get -y remove --purge build-essential
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . .

RUN chmod u+x ./.docker/setup_config.sh

# # This clone puts the server code into the image, not the container
RUN bash -c "./.docker/setup_config.sh"

# #declare environment variables
ENV DB_HOST='db'
ENV WEB_SERVER_HOST=0.0.0.0

ENV LIVERELOAD_SRC=''
ENV STUDY_CONFIG=''
RUN chmod u+x ./.docker/docker_start_script.sh

EXPOSE 8080

CMD ["/bin/bash", "./.docker/docker_start_script.sh"]
