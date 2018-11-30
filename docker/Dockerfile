# python 3
FROM emission-server-base

MAINTAINER Alvin Ghouas

ENV DB_HOST=''
ENV WEB_SERVER_HOST=''

ADD start_script.sh /usr/src/app/start_script.sh
RUN chmod u+x /usr/src/app/start_script.sh

EXPOSE 8080

CMD ["/bin/bash", "/usr/src/app/start_script.sh"]
