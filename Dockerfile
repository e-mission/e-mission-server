# python 3
FROM ubuntu:jammy

MAINTAINER K. Shankari (shankari@eecs.berkeley.edu)

WORKDIR /usr/src/app

RUN apt-get -y -qq update
RUN apt-get install -y -qq curl
RUN apt-get install -y -qq wget
# RUN apt-get install -y git

# install nano and vim for editing
# RUN apt-get -y install nano vim

# install jq to parse json within bash scripts
RUN curl -o /usr/local/bin/jq http://stedolan.github.io/jq/download/linux64/jq && \
  chmod +x /usr/local/bin/jq

# cleanup
RUN apt-get -y remove --purge build-essential
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY . .

RUN chmod u+x ./.docker/setup_config.sh

# # This clone puts the server code into the image, not the container
RUN bash -c "./.docker/setup_config.sh"

# #declare environment variables
ENV DB_HOST=''
ENV WEB_SERVER_HOST=''

ENV LIVERELOAD_SRC=''
ENV STUDY_CONFIG=''
RUN chmod u+x ./.docker/docker_start_script.sh

EXPOSE 8080

CMD ["/bin/bash", "./.docker/docker_start_script.sh"]
