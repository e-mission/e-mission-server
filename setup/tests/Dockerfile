# python 3
FROM emission/e-mission-server.dev.server-only:latest

# CHANGEME: Create the files that correspond to your configuration in the conf directory
# COPY conf/net/auth/google_auth.json /usr/src/app/conf/net/auth/google_auth.json
COPY start_script.sh /start_script.sh

CMD ["/bin/bash", "/start_script.sh"]
