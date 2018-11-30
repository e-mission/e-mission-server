# python 3
FROM continuumio/miniconda3

MAINTAINER Attawit Kittikrairit

# set working directory
WORKDIR /usr/src/app

# clone from repo
RUN git clone https://github.com/e-mission/e-mission-server.git .

# setup python environment
RUN conda env update --name emission --file setup/environment36.yml
RUN /bin/bash -c "source activate emission; pip install six --upgrade"

# install nodejs, npm and bower
RUN apt-get update
RUN apt-get install -y build-essential
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -
RUN apt-get -y install nodejs
RUN npm install -g bower
WORKDIR /usr/src/app/webapp
RUN bower update --allow-root
WORKDIR /usr/src/app

# install nano for editing
RUN apt-get -y install nano vim

# cleanup
RUN apt-get -y remove --purge build-essential
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# start the server
ADD docker/start_script.sh /usr/src/app/start_script.sh
RUN chmod u+x /usr/src/app/start_script.sh

EXPOSE 8080

CMD ["/bin/bash", "/usr/src/app/start_script.sh"]
